import logging
from contextlib import asynccontextmanager
from uuid import UUID, uuid4


from fastapi import Depends, FastAPI, HTTPException, Request
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import engine, get_db
from app.models import Event
from app.schemas import EventCreate, EventResponse, EventAccepted

from app.kafka import publish_event, start_kafka_producer, stop_kafka_producer

from aiokafka.errors import KafkaError


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dislog.ingestion")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_kafka_producer()

    try:
        yield
    finally:
        await stop_kafka_producer()
        await engine.dispose()


app = FastAPI(
    title="DisLog Ingestion API",
    version="0.2.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "dislog-ingestion"}


@app.get("/ready")
async def ready(db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(text("SELECT 1"))
        return {"status": "ready", "database": "connected"}
    except SQLAlchemyError as exc:
        logger.warning("Database readiness check failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="Database unavailable",
        ) from exc


@app.post("/v1/events", response_model=EventAccepted, status_code=202)
async def create_event(
    event: EventCreate,
    request: Request,
):
    request_id = request.headers.get("x-request-id", "unknown")

    event_id = uuid4()

    payload = {
        "id" : str(event_id),
        **event.model_dump(),
    }

    try:
        await publish_event(payload)
    except Exception as exc:
        logger.exception(
            "Event publication failed request_id = %s",
            request_id
        )
        raise HTTPException(
            status_code=503,
            detail = "Event ingestion temporarily not available",
        ) from exc
    
    logger.info(
        "event_accepted request_id=%s event_id=%s service=%s",
        request_id,
        event_id,
        event.service,
    )

    return EventAccepted(id=event_id, status="accepted")


@app.get("/v1/events/{event_id}", response_model=EventResponse)
async def get_event(
    event_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Event).where(Event.id == event_id)
    )
    event = result.scalar_one_or_none()

    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")

    return event

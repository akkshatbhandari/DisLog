
import logging
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import engine, get_db
from app.models import Event
from app.schemas import EventCreate, EventResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scalehub.ingestion")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(
    title="ScaleHub Ingestion API",
    version="0.2.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "scalehub-ingestion"}


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


@app.post("/v1/events", response_model=EventResponse, status_code=201)
async def create_event(
    event: EventCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    request_id = request.headers.get("x-request-id", "unknown")

    db_event = Event(
        service=event.service,
        level=event.level,
        message=event.message,
        attributes=event.attributes,
    )

    try:
        db.add(db_event)
        await db.commit()
        await db.refresh(db_event)
    except SQLAlchemyError as exc:
        await db.rollback()
        logger.exception("Event persistence failed request_id=%s", request_id)
        raise HTTPException(
            status_code=503,
            detail="Could not persist event",
        ) from exc

    logger.info(
        "event_persisted request_id=%s event_id=%s service=%s",
        request_id,
        db_event.id,
        db_event.service,
    )

    return db_event


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

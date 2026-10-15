
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger("DisLog.ingestion")

app = FastAPI(
    title="DisLog Ingestion API",
    version="0.1.0",
)


class EventCreate(BaseModel):
    service: str = Field(min_length=1, max_length=100)
    level: str = Field(min_length=1, max_length=20)
    message: str = Field(min_length=1, max_length=5000)
    attributes: dict[str, Any] = Field(default_factory=dict)


class EventResponse(BaseModel):
    id: UUID
    service: str
    level: str
    message: str
    attributes: dict[str, Any]
    created_at: datetime


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "scalehub-ingestion",
    }


@app.post(
    "/v1/events",
    response_model=EventResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_event(
    event: EventCreate,
    request: Request,
):
    request_id = request.headers.get("x-request-id", "unknown")

    result = EventResponse(
        id=uuid4(),
        service=event.service,
        level=event.level,
        message=event.message,
        attributes=event.attributes,
        created_at=datetime.now(timezone.utc),
    )

    logger.info(
        "event_accepted request_id=%s event_id=%s service=%s level=%s",
        request_id,
        result.id,
        result.service,
        result.level,
    )

    return result

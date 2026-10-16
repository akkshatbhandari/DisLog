
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class EventCreate(BaseModel):
    service: str = Field(min_length=1, max_length=100)
    level: str = Field(min_length=1, max_length=20)
    message: str = Field(min_length=1, max_length=5000)
    attributes: dict[str, Any] = Field(default_factory=dict)


class EventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    service: str
    level: str
    message: str
    attributes: dict[str, Any]
    created_at: datetime

class EventAccepted(BaseModel):
    id: UUID
    status: str = "accepted"
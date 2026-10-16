
import pytest
from pydantic import ValidationError

from app.schemas import EventCreate


def test_valid_event():
    event = EventCreate(
        service="payment-api",
        level="ERROR",
        message="Payment failed",
        attributes={"status_code": 500},
    )

    assert event.service == "payment-api"
    assert event.level == "ERROR"
    assert event.attributes["status_code"] == 500


def test_empty_service_is_rejected():
    with pytest.raises(ValidationError):
        EventCreate(
            service="",
            level="ERROR",
            message="Payment failed",
        )


def test_message_over_limit_is_rejected():
    with pytest.raises(ValidationError):
        EventCreate(
            service="payment-api",
            level="ERROR",
            message="x" * 5001,
        )


def test_attributes_default_to_empty_dict():
    event = EventCreate(
        service="payment-api",
        level="INFO",
        message="Service started",
    )

    assert event.attributes == {}

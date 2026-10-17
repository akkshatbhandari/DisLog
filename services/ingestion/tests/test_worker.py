import json

from uuid import uuid4

import pytest
import app.worker as worker


def valid_payload():
    return {
        "id" : str(uuid4()),
        "service" : "dislog-api",
        "level" : "INFO",
        "message" : "Worker test event",
        "attributes" : {"test" : True},
    }


def test_deserialize_valid_json():
    payload = valid_payload()

    result = worker.deserialize_event(
        json.dumps(payload).encode("utf-8")
    )

    assert result == payload

def test_deserialize_invalid_json():
    with pytest.raises(ValueError, match = "valid UTF-8 JSON"):
        worker.deserialize_event(b"{invalid-json}")

def test_deserialize_non_object_json():
    with pytest.raises(ValueError, match = "JSON object"):
        worker.deserialize_event(b'["not", "an", "object"]')

def test_validate_valid_event():
    payload = valid_payload()


    result = worker.validate_event(payload)

    assert str(result) == payload["id"]

def test_validate_missing_event_id():
    payload = valid_payload()

    del payload["id"]

    with pytest.raises(ValueError, match = "missing required fields"):
        worker.validate_event(payload)

def test_validate_invalid_uuid():
    payload = valid_payload()
    payload["id"] = "not-a-uuid"

    with pytest.raises(ValueError, match = "missing required fields"):
        worker.validate_event(payload)


@pytest.mark.parametrize(
    ("field","value"),
    [
        ("service",""),
        ("service","s" * 101),
        ("level",""),
        ("level","L" * 21),
        ("message",""),
        ("message","m" * 5001),
        ("attributes",[]),
    ],
)

def test_validate_invalid_event_fields(field, value):
    payload = valid_payload()
    payload[field] = value

    with pytest.raises(ValueError):
        worker.validate_event(payload)


from unittest.mock import AsyncMock, MagicMock, patch

def make_mock_session():
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value = session)
    session.__aexit__ = AsyncMock(return_value = False)
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    return session

@pytest.mark.asyncio
async def test_process_event_retries_after_transient_database_failure():
    payload = valid_payload()

    failed_session = make_mock_session()
    failed_session.commit.side_effect = RuntimeError("temporary database failure")
    successful_session = make_mock_session()

    with(
        patch.object(
            worker,
            "SessionLocal",
            side_effect = [failed_session, successful_session],
        ) as session_factory,
        patch.object(worker.asyncio, "sleep", new_callable = AsyncMock) as sleep_mock,
    ):
        await worker.process_event(payload)

    assert session_factory.call_count == 2
    assert failed_session.commit.await_count  == 1
    assert successful_session.commit.await_count == 1

    sleep_mock.assert_awaited_once_with(worker.INITIAL_RETRY_DELAY)

@pytest.mark.asyncio
async def test_process_event_retries_after_retry_exhaustion():
    payload = valid_payload()
    failed_sessions = [make_mock_session() for _ in range(worker.MAX_DB_RETRIES)]

    for session in failed_sessions:
        session.commit.side_effect = RuntimeError("database unavailable")

    with(
        patch.object(worker, "SessionLocal", side_effect = failed_sessions) as session_factory,
        patch.object(worker.asyncio, "sleep", new_callable = AsyncMock) as sleep_mock,
    ):
        with pytest.raises(RuntimeError, match = "database unavailable"):
            await worker.process_event(payload)
    
    assert session_factory.call_count == worker.MAX_DB_RETRIES
    assert sleep_mock.await_count == worker.MAX_DB_RETRIES - 1


@pytest.mark.asyncio
async def test_process_event_uses_duplicate_safe_insert():
    payload = valid_payload()
    session = make_mock_session()

    with patch.object(worker, "SessionLocal", return_value = session):
        await worker.process_event(payload)
    
    session.execute.assert_awaited_once()
    session.commit.assert_awaited_once()

    statement = session.execute.await_args.args[0]

    compiled_sql = str(
        statement.compile(
            dialect = __import__(
                "sqlalchemy.dialects.postgresql",
                fromlist = ["dialect"],
            ).dialect()
        )
    )

    assert "ON CONFLICT" in compiled_sql
    assert "DO NOTHING" in compiled_sql


from types import SimpleNamespace

@pytest.mark.asyncio
async def test_process_record_commits_after_successful_persistence():
    payload = valid_payload()
    record = SimpleNamespace(
        value = json.dumps(payload).encode("utf-8"),
        partition = 0,
        offset = 12,
    )
    dead_letter_producer = SimpleNamespace(
        send_and_wait = AsyncMock()
    )

    consumer = SimpleNamespace(commit = AsyncMock())

    with patch.object(worker, "process_event", new_callable = AsyncMock) as persist:
        await worker.process_record(record, consumer, dead_letter_producer)
    
    persist.assert_awaited_once_with(payload)
    consumer.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_record_does_not_commit_when_persistence_fails():
    payload = valid_payload()
    record = SimpleNamespace(
        value=json.dumps(payload).encode("utf-8"),
        partition=0,
        offset=13,
    )
    dead_letter_producer = SimpleNamespace(
        send_and_wait = AsyncMock()
    )
    consumer = SimpleNamespace(commit=AsyncMock())

    with patch.object(
        worker,
        "process_event",
        new_callable = AsyncMock,
        side_effect = RuntimeError("database unavailable"),
    ):
        with pytest.raises(RuntimeError, match = "database unavailable"):
            await worker.process_record(record, consumer, dead_letter_producer)
    
    consumer.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_record_does_not_commit_malformed_json():
    record = SimpleNamespace(
        topic = "dislog-events",
        value = b"{invalid-json}",
        partition = 0,
        offset = 14,
    )
    dead_letter_producer = SimpleNamespace(
        send_and_wait = AsyncMock()
    )
    consumer = SimpleNamespace(commit = AsyncMock())

    await worker.process_record(record, consumer, dead_letter_producer)
    
    dead_letter_producer.send_and_wait.assert_awaited_once()
    consumer.commit.assert_awaited_once()

@pytest.mark.asyncio
async def test_process_record_does_not_commit_when_dlq_publish_fails():
    record = SimpleNamespace(
        topic = "dislog-events",
        value = b"{invalid-json}",
        partition = 0,
        offset = 15,
    )

    dead_letter_producer = SimpleNamespace(
        send_and_wait = AsyncMock(
            side_effect = RuntimeError("DLQ unavailable")
        )
    )

    consumer = SimpleNamespace(commit = AsyncMock())

    with pytest.raises(RuntimeError, match = "DLQ unavailable"):
        await worker.process_record(
            record,
            consumer,
            dead_letter_producer,
        )
    
    dead_letter_producer.send_and_wait.assert_awaited_once()
    consumer.commit.assert_not_awaited()
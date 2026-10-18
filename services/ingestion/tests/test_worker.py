import json
import asyncio
from uuid import uuid4

import pytest
import app.worker as worker

from unittest.mock import AsyncMock, MagicMock, patch
from aiokafka.structs import TopicPartition

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

    sleep_mock.assert_awaited_once_with(worker.DB_INITIAL_RETRY_DELAY)

@pytest.mark.asyncio
async def test_process_event_retries_after_retry_exhaustion():
    payload = valid_payload()
    failed_sessions = [make_mock_session() for _ in range(worker.DB_MAX_RETRIES)]

    for session in failed_sessions:
        session.commit.side_effect = RuntimeError("database unavailable")

    with(
        patch.object(worker, "SessionLocal", side_effect = failed_sessions) as session_factory,
        patch.object(worker.asyncio, "sleep", new_callable = AsyncMock) as sleep_mock,
    ):
        with pytest.raises(RuntimeError, match = "database unavailable"):
            await worker.process_event(payload)
    
    assert session_factory.call_count == worker.DB_MAX_RETRIES
    assert sleep_mock.await_count == worker.DB_MAX_RETRIES - 1

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
async def test_persistence_worker_processes_queued_event():
    payload = valid_payload()
    record = worker.PendingRecord(
        topic_partition=worker.TopicPartition("dislog-events", 0),
        offset=10,
        payload=payload,
    )
    queue = worker.create_work_queue()
    await queue.put(record)

    with patch.object(
        worker, "process_event", new_callable=AsyncMock
    ) as persist:
        task = asyncio.create_task(
            worker.persistence_worker(queue, worker_id=1)
        )

        # Wait until the queued event has been processed.
        await queue.join()

        # Send the shutdown sentinel.
        await queue.put(None)
        await task

    persist.assert_awaited_once_with(payload)
    assert record.completed is True
    assert queue.empty()


@pytest.mark.asyncio
async def test_persistence_worker_stops_on_sentinel():
    queue = worker.create_work_queue()
    await queue.put(None)

    await worker.persistence_worker(queue, worker_id=1)

    assert queue.empty()


@pytest.mark.asyncio
async def test_persistence_worker_propagates_failure():
    payload = valid_payload()
    record = worker.PendingRecord(
        topic_partition=worker.TopicPartition("dislog-events", 0),
        offset=20,
        payload=payload,
    )
    queue = worker.create_work_queue()
    await queue.put(record)

    with patch.object(
        worker,
        "process_event",
        new_callable=AsyncMock,
        side_effect=RuntimeError("database unavailable"),
    ):
        task = asyncio.create_task(
            worker.persistence_worker(queue, worker_id=1)
        )

        with pytest.raises(
            RuntimeError, match="database unavailable"
        ):
            await task

    assert record.completed is False

    # The failed queue item must not leave queue.join() hanging.
    await asyncio.wait_for(queue.join(), timeout=1)

@pytest.mark.asyncio
async def test_supervisor_propagates_worker_failure():
    queue = worker.create_work_queue()
    dead_letter_producer = AsyncMock()
    offset_tracker = AsyncMock()

    with patch.object(
        worker,
        "persistence_worker",
        new_callable=AsyncMock,
        side_effect=RuntimeError("worker crashed"),
    ):
        with pytest.raises(RuntimeError, match="worker crashed"):
            await worker.supervise_workers(
                queue,
                dead_letter_producer,
                offset_tracker,
                )

@pytest.mark.asyncio
async def test_offset_tracker_waits_for_earlier_record():
    consumer = AsyncMock()
    tracker = worker.OffsetTracker(consumer)
    partition = TopicPartition("dislog-events", 0)

    first = worker.PendingRecord(
        topic_partition=partition,
        offset=10,
        payload={},
    )
    second = worker.PendingRecord(
        topic_partition=partition,
        offset=11,
        payload={},
    )

    tracker.register(first)
    tracker.register(second)

    # Offset 11 completes first; nothing can be committed yet.
    await tracker.mark_completed(second)
    consumer.commit.assert_not_awaited()

    # Once offset 10 completes, both records can be committed.
    await tracker.mark_completed(first)

    consumer.commit.assert_awaited_once()
    committed = consumer.commit.await_args.args[0]
    assert committed[partition].offset == 12
    assert partition not in tracker.pending


@pytest.mark.asyncio
async def test_offset_tracker_retains_records_when_commit_fails():
    consumer = AsyncMock()
    consumer.commit.side_effect = RuntimeError("Kafka commit failed")

    tracker = worker.OffsetTracker(consumer)
    partition = worker.TopicPartition("dislog-events", 0)

    item = worker.PendingRecord(
        topic_partition=partition,
        offset=20,
        payload={},
    )
    tracker.register(item)

    with patch.object(worker.asyncio, "sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="Kafka commit failed"):
            await tracker.mark_completed(item)

    assert consumer.commit.await_count == worker.KAFKA_COMMIT_MAX_RETRIES
    assert partition in tracker.pending
    assert tracker.pending[partition] == [item]
    assert item.completed is True

@pytest.mark.asyncio
async def test_offset_tracker_commits_partitions_independently():
    consumer = AsyncMock()
    tracker = worker.OffsetTracker(consumer)

    partition_0 = TopicPartition("dislog-events", 0)
    partition_1 = TopicPartition("dislog-events", 1)

    record_0 = worker.PendingRecord(
        topic_partition=partition_0,
        offset=5,
        payload={},
    )
    record_1 = worker.PendingRecord(
        topic_partition=partition_1,
        offset=8,
        payload={},
    )

    tracker.register(record_0)
    tracker.register(record_1)

    await tracker.mark_completed(record_1)

    consumer.commit.assert_awaited_once()
    committed = consumer.commit.await_args.args[0]
    assert committed[partition_1].offset == 9
    assert partition_0 in tracker.pending


@pytest.mark.asyncio
async def test_offset_tracker_retries_transient_commit_failure():
    consumer = AsyncMock()
    consumer.commit.side_effect = [
        RuntimeError("Temporary Kafka failure"),
        None,
    ]

    tracker = worker.OffsetTracker(consumer)
    partition = worker.TopicPartition("dislog-events", 0)

    item = worker.PendingRecord(
        topic_partition=partition,
        offset=21,
        payload={},
    )
    tracker.register(item)

    with patch.object(worker.asyncio, "sleep", new_callable=AsyncMock):
        await tracker.mark_completed(item)

    assert consumer.commit.await_count == 2
    assert partition not in tracker.pending

#Regression testing
@pytest.mark.asyncio
async def test_failed_earlier_offset_blocks_later_commits():
    consumer = AsyncMock()
    tracker = worker.OffsetTracker(consumer)
    partition = TopicPartition("dislog-events", 0)

    offset_10 = worker.PendingRecord(
        topic_partition=partition,
        offset=10,
        payload={},
    )
    offset_11 = worker.PendingRecord(
        topic_partition=partition,
        offset=11,
        payload={},
    )
    offset_12 = worker.PendingRecord(
        topic_partition=partition,
        offset=12,
        payload={},
    )

    for item in (offset_10, offset_11, offset_12):
        tracker.register(item)

    # Simulate offsets 11 and 12 succeeding while 10 remains unfinished.
    await tracker.mark_completed(offset_11)
    await tracker.mark_completed(offset_12)

    consumer.commit.assert_not_awaited()
    assert len(tracker.pending[partition]) == 3

    # Offset 10 eventually succeeds after retry.
    await tracker.mark_completed(offset_10)

    consumer.commit.assert_awaited_once()
    committed = consumer.commit.await_args.args[0]
    assert committed[partition].offset == 13
    assert partition not in tracker.pending

@pytest.mark.asyncio
async def test_worker_persists_event_before_marking_complete():
    queue = asyncio.Queue()
    partition = worker.TopicPartition("dislog-events", 0)
    item = worker.PendingRecord(
        topic_partition=partition,
        offset=12,
        payload=valid_payload(),
    )
    await queue.put(item)

    tracker = SimpleNamespace(
        mark_completed=AsyncMock()
    )

    with patch.object(
        worker, "process_event", new_callable=AsyncMock
    ) as persist:
        task = asyncio.create_task(
            worker.persistence_worker(
                queue,
                worker_id=1,
                offset_tracker=tracker,
            )
        )

        await asyncio.wait_for(queue.join(), timeout=2)

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    persist.assert_awaited_once_with(item.payload)
    tracker.mark_completed.assert_awaited_once_with(item)

@pytest.mark.asyncio
async def test_worker_does_not_mark_complete_when_persistence_fails():
    queue = asyncio.Queue()
    partition = worker.TopicPartition("dislog-events", 0)
    item = worker.PendingRecord(
        topic_partition=partition,
        offset=13,
        payload=valid_payload(),
    )
    await queue.put(item)

    tracker = SimpleNamespace(
        mark_completed=AsyncMock()
    )

    with patch.object(
        worker,
        "process_event",
        new_callable=AsyncMock,
        side_effect=RuntimeError("database unavailable"),
    ):
        with pytest.raises(RuntimeError, match="database unavailable"):
            await worker.persistence_worker(
                queue,
                worker_id=1,
                offset_tracker=tracker,
            )

    tracker.mark_completed.assert_not_awaited()
    assert item.completed is False

@pytest.mark.asyncio
async def test_worker_finishes_current_item_before_shutdown():
    queue = asyncio.Queue()
    partition = worker.TopicPartition("dislog-events", 0)

    item = worker.PendingRecord(
        topic_partition = partition,
        offset = 30,
        payload = valid_payload(),
    )

    await queue.put(item)
    await queue.put(None)

    with patch.object(
        worker,
        "process_event",
        new_callable = AsyncMock,
    ) as persist:
        task = asyncio.create_task(
            worker.persistence_worker(
                queue,
                worker_id = 1,
            )
        )

        await asyncio.wait_for(task, timeout = 2)
        await asyncio.wait_for(queue.join(), timeout = 2)

    persist.assert_awaited_once_with(item.payload)
    assert item.completed is True
    assert queue.empty()


@pytest.mark.asyncio
async def test_supervisor_drains_queue_before_graceful_shutdown():
    queue = worker.create_work_queue()
    shutdown_event = asyncio.Event()

    # Mock the Kafka consumer, but use the real OffsetTracker.
    consumer = AsyncMock()
    offset_tracker = worker.OffsetTracker(consumer)

    item = worker.PendingRecord(
        topic_partition=worker.TopicPartition("dislog-events", 0),
        offset=41,
        payload=valid_payload(),
    )
    # Register the record before processing so the tracker
    # can safely determine which Kafka offset to commit.
    offset_tracker.register(item)
    await queue.put(item)

    with patch.object(
        worker,
        "process_event",
        new_callable=AsyncMock,
    ) as persist:
        supervisor_task = asyncio.create_task(
            worker.supervise_workers(
                queue,
                dead_letter_producer=None,
                offset_tracker=offset_tracker,
                shutdown_event=shutdown_event,
            )
        )
        # Request graceful shutdown
        shutdown_event.set()

        # The supervisor should drain the queue and stop its workers.
        await asyncio.wait_for(supervisor_task, timeout=3)

    # Verify that the event was persisted exactly once.
    persist.assert_awaited_once_with(item.payload)

    # Verify that the real tracker marked the event as completed.
    assert item.completed is True

    # Verify that the tracker attempted to commit the Kafka offset.
    consumer.commit.assert_awaited_once()

    # Verify that shutdown left no queued or unfinished work.
    assert queue.empty()
    await asyncio.wait_for(queue.join(), timeout=1)

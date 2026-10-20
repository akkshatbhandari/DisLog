
import asyncio
import time
from uuid import uuid4

import pytest
from aiokafka import TopicPartition
from sqlalchemy import delete, select

import app.worker as worker
from app.database import SessionLocal
from app.models import Event


EVENT_COUNT = 1000


@pytest.mark.asyncio
async def test_sustained_load_through_production_worker():
    run_id = str(uuid4())
    event_ids = [str(uuid4()) for _ in range(EVENT_COUNT)]

    queue = worker.create_work_queue()
    partition = TopicPartition(worker.KAFKA_TOPIC, 0)

    async def produce_events():
        for offset, event_id in enumerate(event_ids):
            payload = {
                "id": event_id,
                "service": "dislog-sustained-load-test",
                "level": "INFO",
                "message": f"Sustained-load event {offset}",
                "attributes": {
                    "test_run": run_id,
                    "sequence": offset,
                },
            }

            await queue.put(
                worker.PendingRecord(
                    topic_partition=partition,
                    offset=offset,
                    payload=payload,
                )
            )

    worker_task = asyncio.create_task(
        worker.persistence_worker(queue, worker_id=999)
    )

    start = time.perf_counter()

    try:
        await produce_events()
        await queue.join()

        elapsed = time.perf_counter() - start

        # Stop the worker after all events have been processed.
        await queue.put(None)
        await asyncio.wait_for(worker_task, timeout=30)

        # Verify that every event from this run exists in PostgreSQL.
        async with SessionLocal() as session:
            result = await session.execute(
                select(Event.id).where(Event.id.in_(event_ids))
            )
            persisted_ids = {
                str(event_id)
                for event_id in result.scalars().all()
            }

        assert persisted_ids == set(event_ids), (
            f"Expected {EVENT_COUNT} events, "
            f"found {len(persisted_ids)}"
        )

        throughput = EVENT_COUNT / elapsed if elapsed > 0 else 0

        print("\nSustained-load worker test")
        print("-" * 40)
        print(f"Events submitted: {EVENT_COUNT}")
        print(f"Events persisted: {len(persisted_ids)}")
        print(f"Elapsed time: {elapsed:.3f} seconds")
        print(f"Throughput: {throughput:.2f} events/sec")

    finally:
        # Ensure the worker does not remain running after a failure.
        if not worker_task.done():
            await queue.put(None)
            try:
                await asyncio.wait_for(worker_task, timeout=30)
            except Exception:
                worker_task.cancel()
                await asyncio.gather(worker_task, return_exceptions=True)

        # Delete only records created by this test.
        async with SessionLocal() as session:
            await session.execute(
                delete(Event).where(Event.id.in_(event_ids))
            )
            await session.commit()

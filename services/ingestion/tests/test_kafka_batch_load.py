
import asyncio
import json
import time
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import delete, select

from app.database import SessionLocal
from app.models import Event
from app.worker import process_event_batch


BROKER = "localhost:9092"
TOPIC = "dislog-recovery-test"
BATCH_SIZE = 100


@pytest.mark.asyncio
async def test_kafka_burst_batch_persistence_load():
    group_id = f"dislog-batch-load-{uuid4().hex}"
    run_id = str(uuid4())
    event_ids = [str(uuid4()) for _ in range(BATCH_SIZE)]

    producer = AIOKafkaProducer(
        bootstrap_servers=BROKER,
        acks="all",
    )

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )

    producer_started = False
    consumer_started = False

    try:
        await producer.start()
        producer_started = True

        await consumer.start()
        consumer_started = True

        # Wait for partition assignment.
        for _ in range(100):
            if consumer.assignment():
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("Consumer did not receive partition assignment")

        # Ignore previously existing records.
        await consumer.seek_to_end()
        await asyncio.sleep(0.2)

        events = [
            {
                "id": event_id,
                "service": "dislog-batch-load-test",
                "level": "INFO",
                "message": f"Batch event {index}",
                "attributes": {
                    "test_run": run_id,
                    "sequence": index,
                },
            }
            for index, event_id in enumerate(event_ids)
        ]

        # Publish events to Kafka.
        publish_start = time.perf_counter()

        await asyncio.gather(
            *(
                producer.send_and_wait(
                    TOPIC,
                    json.dumps(event).encode("utf-8"),
                )
                for event in events
            )
        )

        publish_seconds = time.perf_counter() - publish_start
        produced_ids = set(event_ids)

        # Receive all events belonging to this test run.
        receive_start = time.perf_counter()
        received = {}
        deadline = time.perf_counter() + 30

        while len(received) < BATCH_SIZE:
            remaining = deadline - time.perf_counter()

            if remaining <= 0:
                pytest.fail(
                    f"Timed out waiting for events: "
                    f"{len(received)}/{BATCH_SIZE}"
                )

            records = await asyncio.wait_for(
                consumer.getmany(
                    timeout_ms=500,
                    max_records=BATCH_SIZE,
                ),
                timeout=remaining,
            )

            for partition_records in records.values():
                for record in partition_records:
                    payload = json.loads(record.value)

                    if (
                        payload.get("attributes", {}).get("test_run")
                        == run_id
                    ):
                        received[payload["id"]] = payload

        assert set(received) == produced_ids

        receive_seconds = time.perf_counter() - receive_start

        # Persist all 100 events through ONE batch-processing call.
        persistence_start = time.perf_counter()

        await process_event_batch(list(received.values()))

        persistence_seconds = time.perf_counter() - persistence_start

        # Verify persistence in PostgreSQL.
        async with SessionLocal() as session:
            result = await session.execute(
                select(Event.id).where(Event.id.in_(event_ids))
            )
            persisted_ids = {
                str(event_id)
                for event_id in result.scalars().all()
            }

        assert persisted_ids == produced_ids, (
            f"Expected {BATCH_SIZE} persisted events, "
            f"found {len(persisted_ids)}"
        )

        total_seconds = (
            publish_seconds + receive_seconds + persistence_seconds
        )

        throughput = (
            BATCH_SIZE / total_seconds
            if total_seconds > 0
            else float("inf")
        )

        print("\nKafka batch persistence load test")
        print("-" * 40)
        print(f"Batch size: {BATCH_SIZE}")
        print(f"Published events: {len(produced_ids)}")
        print(f"Received events: {len(received)}")
        print(f"Persisted events: {len(persisted_ids)}")
        print(f"Kafka publish time: {publish_seconds:.3f} seconds")
        print(f"Kafka receive time: {receive_seconds:.3f} seconds")
        print(f"Batch persistence time: {persistence_seconds:.3f} seconds")
        print(f"Total time: {total_seconds:.3f} seconds")
        print(f"End-to-end throughput: {throughput:.2f} events/sec")

    finally:
        if consumer_started:
            await consumer.stop()

        if producer_started:
            await producer.stop()

        # Delete only this test's records.
        async with SessionLocal() as session:
            await session.execute(
                delete(Event).where(Event.id.in_(event_ids))
            )
            await session.commit()

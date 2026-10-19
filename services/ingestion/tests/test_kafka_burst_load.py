import asyncio
import json
import time
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import delete, func, select

from app.database import SessionLocal
from app.models import Event
from app.worker import process_event


BROKER = "localhost:9092"
TOPIC = "dislog-recovery-test"
BATCH_SIZE = 100


@pytest.mark.asyncio
async def test_kafka_burst_persistence_load():
    group_id = f"dislog-burst-load-{uuid4().hex}"
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

    started = False
    produced_ids = set()
    persisted_ids = set()

    try:
        await producer.start()
        await consumer.start()
        started = True

        # Establish assignment before publishing the batch.
        for _ in range(100):
            if consumer.assignment():
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("Load-test consumer did not receive partition assignment")

        # Begin at the end of the topic so older test records are ignored.
        await consumer.seek_to_end()
        await asyncio.sleep(0.2)

        events = [
            {
                "id": event_id,
                "service": "dislog-burst-load-test",
                "level": "INFO",
                "message": f"Burst event {index}",
                "attributes": {
                    "test_run": run_id,
                    "sequence": index,
                },
            }
            for index, event_id in enumerate(event_ids)
        ]

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

        # Read the published events and persist them.
        persistence_start = time.perf_counter()
        received = {}

        deadline = time.perf_counter() + 30

        while len(received) < BATCH_SIZE:
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                pytest.fail(
                    f"Timed out waiting for events: "
                    f"received {len(received)}/{BATCH_SIZE}"
                )

            records = await asyncio.wait_for(
                consumer.getmany(timeout_ms=500, max_records=BATCH_SIZE),
                timeout=remaining,
            )

            for partition_records in records.values():
                for record in partition_records:
                    payload = json.loads(record.value)
                    if payload.get("attributes", {}).get("test_run") == run_id:
                        received[payload["id"]] = payload

        assert set(received) == produced_ids

        # Process events concurrently to simulate multiple persistence workers.
        await asyncio.gather(
            *(process_event(payload) for payload in received.values())
        )

        async with SessionLocal() as session:
            result = await session.execute(
                select(Event.id).where(Event.id.in_(event_ids))
            )
            persisted_ids = {str(event_id) for event_id in result.scalars().all()}

        persistence_seconds = time.perf_counter() - persistence_start

        assert persisted_ids == produced_ids

        total_seconds = publish_seconds + persistence_seconds
        throughput = (
            BATCH_SIZE / total_seconds if total_seconds > 0 else float("inf")
        )

        print(f"\nBatch size: {BATCH_SIZE}")
        print(f"Published events: {len(produced_ids)}")
        print(f"Persisted events: {len(persisted_ids)}")
        print(f"Kafka publish time: {publish_seconds:.3f} seconds")
        print(f"Receive + persistence time: {persistence_seconds:.3f} seconds")
        print(f"Approximate end-to-end batch throughput: {throughput:.2f} events/sec")

    finally:
        if started:
            await consumer.stop()
        else:
            # AIOKafkaConsumer may need stopping only if start succeeded.
            pass

        await producer.stop()

        async with SessionLocal() as session:
            await session.execute(
                delete(Event).where(Event.id.in_(event_ids))
            )
            await session.commit()

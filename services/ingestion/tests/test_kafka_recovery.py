import asyncio
import json
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import delete, func, select

from app.database import SessionLocal
from app.models import Event
from app.worker import process_event

BROKER = "localhost:9092"
TOPIC = "dislog-recovery-test"

@pytest.mark.asyncio
async def test_kafka_redelivers_uncommitted_event_without_duplicate_row():
    group_id = f"dislog-recovery-{uuid4().hex}"
    event_id = str(uuid4())

    payload = {
        "id": event_id,
        "service": "dislog-recovery-test",
        "level": "INFO",
        "message": "Crash recovery integration test",
        "attributes": {"test": True},
    }

    producer = AIOKafkaProducer(
        bootstrap_servers=BROKER,
        acks="all",
    )

    first_consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )

    second_consumer = None

    try:
        await producer.start()
        await first_consumer.start()

        # Establish the consumer assignment and starting position
        # before publishing the test event.
        for _ in range(100):
            if first_consumer.assignment():
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("First consumer did not receive a partition assignment")

        await first_consumer.seek_to_end()
        await asyncio.sleep(0.2)

        published_offset = await producer.send_and_wait(
            TOPIC,
            json.dumps(payload).encode("utf-8"),
        )

        # First delivery: receive the event, but simulate a crash
        # before persistence and before committing its offset.
        first_record = await asyncio.wait_for(
            first_consumer.getone(),
            timeout=10,
        )

        assert first_record.offset == published_offset.offset
        assert json.loads(first_record.value)["id"] == event_id

        # Stop the first consumer without committing.
        await first_consumer.stop()

        # Restart with the SAME group ID. Kafka should redeliver
        # the uncommitted event.
        second_consumer = AIOKafkaConsumer(
            TOPIC,
            bootstrap_servers=BROKER,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await second_consumer.start()

        for _ in range(100):
            if second_consumer.assignment():
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("Second consumer did not receive a partition assignment")

        partition = next(iter(second_consumer.assignment()))
        second_consumer.seek(partition, published_offset.offset)

        redelivered = await asyncio.wait_for(
            second_consumer.getone(),
            timeout=15,
        )

        assert redelivered.offset == published_offset.offset
        assert json.loads(redelivered.value)["id"] == event_id

        # Persist the redelivered event twice to verify that
        # PostgreSQL idempotency also protects against duplicates.
        redelivered_payload = json.loads(redelivered.value)
        await process_event(redelivered_payload)
        await process_event(redelivered_payload)

        async with SessionLocal() as session:
            result = await session.execute(
                select(func.count())
                .select_from(Event)
                .where(Event.id == event_id)
            )
            assert result.scalar_one() == 1

    finally:
        if second_consumer is not None:
            await second_consumer.stop()

        # The first consumer may already have been stopped above.
        # Stopping it again is safe to attempt but may raise.
        try:
            await first_consumer.stop()
        except Exception:
            pass

        await producer.stop()

        async with SessionLocal() as session:
            await session.execute(
                delete(Event).where(Event.id == event_id)
            )
            await session.commit()

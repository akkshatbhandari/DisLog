import asyncio
import json
import sys
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import delete, func, select

from app.database import SessionLocal
from app.models import Event
from app.worker import process_event


BROKER = "localhost:9092"
TOPIC = "dislog-recovery-test"

# This separate Python process receives an event, reports its offset,
# and then waits forever without persisting or committing the event.
CHILD_SCRIPT = r"""
import asyncio
import json
import sys

from aiokafka import AIOKafkaConsumer

TOPIC = sys.argv[1]
GROUP_ID = sys.argv[2]


async def main():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        session_timeout_ms=6000,
        heartbeat_interval_ms=2000,
    )

    await consumer.start()
    try:
        for _ in range(150):
            if consumer.assignment():
                break
            await asyncio.sleep(0.1)
        else:
            raise RuntimeError("Child consumer received no partition assignment")

        record = await asyncio.wait_for(consumer.getone(), timeout=25)
        payload = json.loads(record.value)

        print(
            f"RECEIVED:{record.offset}:{payload['id']}",
            flush=True,
        )

        # The parent will kill this process while it is waiting.
        # No database write and no Kafka offset commit occur here.
        await asyncio.Event().wait()
    finally:
        await consumer.stop()


asyncio.run(main())
"""


@pytest.mark.asyncio
async def test_process_crash_redelivers_event_without_duplicate_row():
    group_id = f"dislog-process-crash-{uuid4().hex}"
    event_id = str(uuid4())

    payload = {
        "id": event_id,
        "service": "dislog-process-crash-test",
        "level": "INFO",
        "message": "Real process crash recovery test",
        "attributes": {"test": True},
    }

    producer = AIOKafkaProducer(
        bootstrap_servers=BROKER,
        acks="all",
    )

    initializer = None
    second_consumer = None
    child = None

    try:
        # Establish a committed starting offset before publishing.
        initializer = AIOKafkaConsumer(
            TOPIC,
            bootstrap_servers=BROKER,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            session_timeout_ms=6000,
            heartbeat_interval_ms=2000,
        )
        await initializer.start()

        for _ in range(100):
            if initializer.assignment():
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("Initializer did not receive a partition assignment")

        await initializer.seek_to_end()
        await initializer.commit()
        await initializer.stop()
        initializer = None

        await producer.start()

        published = await producer.send_and_wait(
            TOPIC,
            json.dumps(payload).encode("utf-8"),
        )

        # Launch a real child process using the same consumer group.
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "-u",
            "-c",
            CHILD_SCRIPT,
            TOPIC,
            group_id,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            marker = await asyncio.wait_for(
                child.stdout.readline(),
                timeout=30,
            )
        except asyncio.TimeoutError:
            child.kill()
            _, stderr = await child.communicate()
            pytest.fail(
                "Child did not receive the event. "
                f"Child stderr: {stderr.decode(errors='replace')}"
            )

        if not marker:
            _, stderr = await child.communicate()
            pytest.fail(
                "Child exited before reporting the event. "
                f"Child stderr: {stderr.decode(errors='replace')}"
            )

        marker_text = marker.decode().strip()
        assert marker_text == f"RECEIVED:{published.offset}:{event_id}"

        # Simulate an abrupt process crash. The child has received the
        # event but has not committed its offset or persisted it.
        child.kill()
        await child.wait()
        child = None

        # Allow Kafka to detect the dead consumer and release its group.
        await asyncio.sleep(8)

        # Restart in the same group. Do not manually seek to the event.
        second_consumer = AIOKafkaConsumer(
            TOPIC,
            bootstrap_servers=BROKER,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            session_timeout_ms=6000,
            heartbeat_interval_ms=2000,
        )
        await second_consumer.start()

        for _ in range(150):
            if second_consumer.assignment():
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("Restarted consumer did not receive a partition assignment")

        redelivered = await asyncio.wait_for(
            second_consumer.getone(),
            timeout=25,
        )

        assert redelivered.offset == published.offset
        redelivered_payload = json.loads(redelivered.value)
        assert redelivered_payload["id"] == event_id

        # Persist twice to verify the database's idempotency protection.
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
        if child is not None and child.returncode is None:
            child.kill()
            await child.wait()

        if initializer is not None:
            await initializer.stop()

        if second_consumer is not None:
            await second_consumer.stop()

        await producer.stop()

        async with SessionLocal() as session:
            await session.execute(
                delete(Event).where(Event.id == event_id)
            )
            await session.commit()

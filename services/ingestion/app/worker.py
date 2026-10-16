import asyncio
import logging
import os
from uuid import UUID

from aiokafka import AIOKafkaConsumer
from sqlalchemy.dialects.postgresql import insert

from app.database import SessionLocal, engine
from app.models import Event


logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("dislog.worker")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dislog-events")
KAFKA_GROUP_ID = "dislog-persistence-v1"

async def process_event(payload: dict) -> None:
    """Persist one kafka event to postgresql idempotently"""

    event_id = UUID(payload["id"])

    statement = (
        insert(Event)
        .values(
            id=event_id,
            service=payload["service"],
            level=payload["level"],
            message=payload["message"],
            attributes=payload.get("attributes", {}),
        )
        .on_conflict_do_nothing(index_elements=[Event.id])
    )

    async with SessionLocal() as session:
        await session.execute(statement)
        await session.commit()

async def run_worker() -> None:
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        group_id = KAFKA_GROUP_ID,
        enable_auto_commit = False,
        auto_offset_reset = "latest",
        value_deserializer = lambda value: __import__("json").loads(
            value.decode("utf-8")
        ),
    )

    await consumer.start()

    logger.info("DisLog persistence worker started")

    try:
        async for record in consumer:
            await process_event(record.value)
            
            await consumer.commit()

            logger.info(
                "event_persisted event_id = %s partition = %s offset = %s",
                record.value["id"],
                record.partition,
                record.offset,
            )
    finally:
        await consumer.stop()
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(run_worker())
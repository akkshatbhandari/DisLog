import asyncio
import json
import logging
import os
from uuid import UUID

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.dialects.postgresql import insert

from app.database import SessionLocal, engine
from app.models import Event


logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("dislog.worker")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC", 
    "dislog-events"
)
KAFKA_GROUP_ID = "dislog-persistence-v1"

DEAD_LETTER_TOPIC = os.getenv(
    "KAFKA_DEAD_LETTER_TOPIC",
    "dislog-events-dlq",
)

MAX_DB_RETRIES = 5
INITIAL_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 16.0

def deserialize_event(value: bytes) -> dict:
    """Decode JSON without allowing malformed payloads to crash deserialization"""

    try:
        payload = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Message is not valid UTF-8 JSON") from exc

    if not isinstance(payload, dict):
        raise ValueError("Event payload must be a JSON object")

    return payload


def validate_event(payload: dict) -> UUID:
    """Validate the fields required for persistence"""
    try:
        event_id = UUID(payload["id"])
        service = payload["service"]
        level = payload["level"]
        message = payload["message"]
        attributes = payload.get("attributes", {})
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Event is missing required fields") from exc

    if not isinstance(service, str) or not 1 <= len(service) <= 100:
        raise ValueError("Invalid service")
    if not isinstance(level, str) or not 1 <= len(level) <= 20:
        raise ValueError("Invalid level")
    if not isinstance(message, str) or not 1 <= len(message) <= 5000:
        raise ValueError("Invalid message")

    if not isinstance(attributes, dict):
        raise ValueError("Event attributes must be a JSON objects")

    return event_id


async def process_event(payload: dict) -> None:
    """Persist one kafka event to postgresql idempotently"""

    event_id = validate_event(payload)

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

    for attempt in range(MAX_DB_RETRIES):
        try:
            async with SessionLocal() as session:
                await session.execute(statement)
                await session.commit()
            return
        except Exception:
            if attempt == MAX_DB_RETRIES - 1:
                logger.exception(
                    "event_persistence_failed event_id=%s attempts=%s",
                    event_id,
                    MAX_DB_RETRIES,
                )
                raise

            delay = min(
                INITIAL_RETRY_DELAY * (2 ** attempt),
                MAX_RETRY_DELAY,
            )
            logger.warning(
                "event_persistence_retry event_id=%s attempt=%s delay_seconds=%s",
                event_id,
                attempt + 1,
                delay,
            )
            await asyncio.sleep(delay)


async def process_record(record, consumer, dead_letter_producer) -> None:
    """Persist valid events or route malformed events to the DLQ."""
    try:
        payload = deserialize_event(record.value)   
        await process_event(payload)
    except ValueError as exc:
        #Preserve malformed messages for later inspection.
        dead_letter_payload = {
            "source_topic" : record.topic,
            "source_partition": record.partition,
            "source_offset": record.offset,
            "error": str(exc),
            "raw_value": (
                record.value.decode("utf-8", errors = "replace")
                if record.value is not None
                else None
            ),
        }

        await dead_letter_producer.send_and_wait(
            DEAD_LETTER_TOPIC,
            json.dumps(dead_letter_payload).encode("utf-8"),
        )
        await consumer.commit()

        logger.warning(
            "event_sent_to_dlq topic=%s partition=%s offset=%s",
            record.topic,
            record.partition,
            record.offset,
        )
        return
    
    await consumer.commit()

    logger.info(
        "event_persisted event_id=%s partition=%s offset=%s",
        payload["id"],
        record.partition,
        record.offset,
    )


async def run_worker() -> None:
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        group_id = KAFKA_GROUP_ID,
        enable_auto_commit = False,
        auto_offset_reset = "latest",

        # Deserialize inside the processing loop so malformed messages
        # can be handled explicitly instead of crashing the consumer.

        value_deserializer = None,
    )

    dead_letter_producer = AIOKafkaProducer(
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        acks = "all"
    )

    await dead_letter_producer.start()

    await consumer.start()
    logger.info("DisLog persistence worker started topic = %s",KAFKA_TOPIC)

    try:
        async for record in consumer:
            try:
                await process_record(record, consumer, dead_letter_producer)
            except ValueError:
                logger.exception(
                    "invalid_event_payload partition=%s offset=%s",
                    record.partition,
                    record.offset,
                )
                raise

            await consumer.commit()

            logger.info(
                "event_persisted event_id = %s partition = %s offset = %s",
                payload["id"],
                record.partition,
                record.offset,
            )
    finally:
        await consumer.stop()
        await dead_letter_producer.stop()
        await engine.dispose()
        logger.info("dislog_persistence_worker_stopped")

if __name__ == "__main__":
    asyncio.run(run_worker())

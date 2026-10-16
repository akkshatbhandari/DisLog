import json
import os

from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)
KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "dislog-events"
)

producer: AIOKafkaProducer | None = None

async def start_kafka_producer() -> None:
    """Create and start the shared kafka producer"""

    global producer

    if producer is not None:
        return 
    
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        value_serializer = lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        enable_idempotence = True,
    )

    await kafka_producer.start()

    producer = kafka_producer


async def stop_kafka_producer() -> None:
    """Flush pending messages and close the producer gracefully"""

    global producer

    if producer is not None:
        kafka_producer = producer
        producer = None
        await kafka_producer.stop()


async def publish_event(event: dict) -> None:
    """Publish an event and wait for kafka's acknowledgment"""

    if producer is None:
        raise RuntimeError("Kafka producer is not running")

    await producer.send_and_wait(KAFKA_TOPIC, event)
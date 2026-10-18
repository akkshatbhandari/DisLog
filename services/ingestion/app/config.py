import os

from pathlib import Path
from dotenv import load_dotenv

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(ENV_FILE)


def positive_int(name: str, default:int) -> int:
    """Read an environment variable as a positive int"""

    raw_val = os.getenv(name, str(default))


    try:
        val = int(raw_val)
    except ValueError as exc:
        raise ValueError(
            f"{name} must be a positive integer"
        ) from exc
    
    if val <= 0:
        raise ValueError(f"{name} must be greater than zero")
    
    return val

def positive_float(name: str, default: float) -> float:
    """Read an environment variable as a positive float"""

    raw_val = os.getenv(name, str(default))

    try:
        val = float(raw_val)
    except ValueError as exc:
        raise ValueError(
            f"{name} must be a positive float"
        ) from exc
    
    if val <= 0:
        raise ValueError(f"{name} must be greater than zero")
    
    return val


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dislog-events")

KAFKA_DEAD_LETTER_TOPIC = os.getenv("KAFKA_DEAD_LETTER_TOPIC","dislog-events-dlq")

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "dislog-persistence-v1"
)

WORKER_COUNT = positive_int("KAFKA_WORKER_COUNT", 4)
QUEUE_MAX_SIZE = positive_int("KAFKA_QUEUE_MAX_SIZE", 1000)

DB_MAX_RETRIES = positive_int("DB_MAX_RETRIES", 5)
DB_INITIAL_RETRY_DELAY = positive_float("DB_INITIAL_RETRY_DELAY", 1.0)
DB_MAX_RETRY_DELAY = positive_float("DB_MAX_RETRY_DELAY", 16.0)


if DB_MAX_RETRY_DELAY < DB_INITIAL_RETRY_DELAY:
    raise ValueError(
        "DB_MAX_RETRY_DELAY must be greater than or equal to "
        "DB_INITIAL_RETRY_DELAY"
    )


KAFKA_COMMIT_MAX_RETRIES = positive_int(
    "KAFKA_COMMIT_MAX_RETRIES", 3
)
KAFKA_COMMIT_RETRY_DELAY = positive_float(
    "KAFKA_COMMIT_RETRY_DELAY", 0.5
)
import asyncio
import json
import logging
import os
import signal
import time

from uuid import UUID

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.structs import OffsetAndMetadata
from sqlalchemy.dialects.postgresql import insert

from app.database import SessionLocal, engine
from app.models import Event

from typing import Any
from collections import defaultdict
from dataclasses import dataclass, field


logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("dislog.worker")

BATCH_SIZE = 25
BATCH_TIMEOUT_SECONDS = 0.1

from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_DEAD_LETTER_TOPIC,
    KAFKA_GROUP_ID,
    WORKER_COUNT,
    QUEUE_MAX_SIZE,
    DB_MAX_RETRIES,
    DB_INITIAL_RETRY_DELAY,
    DB_MAX_RETRY_DELAY,
    KAFKA_COMMIT_MAX_RETRIES,
    KAFKA_COMMIT_RETRY_DELAY,
)



@dataclass
class PendingRecord:
    """Track one kafka record until processing complete"""

    topic_partition: TopicPartition
    offset: int
    payload: Any
    completed: bool = False
    topic: str = KAFKA_TOPIC

class OffsetTracker:
    """Commit only offsets whose earlier dispatched records have completed."""

    def __init__(self, consumer):
        self.consumer = consumer
        self.pending = defaultdict(list)
        self.lock = asyncio.Lock()

    def register(self, item: PendingRecord) -> None:
        """Register a record before placing it on the worker queue."""
        self.pending[item.topic_partition].append(item)

    async def mark_completed(self, item: PendingRecord) -> None:
        """Commit the contiguous completed prefix for this partition."""

        async with self.lock:
            item.completed = True
            records = self.pending[item.topic_partition]

            completed_count = 0
            for record in records:
                if not record.completed:
                    break
                completed_count += 1
            
            if completed_count == 0:
                return
            
            last_record = records[completed_count - 1]
            next_offset = last_record.offset + 1

            commit_offsets = {
                item.topic_partition: OffsetAndMetadata(next_offset, "")
            }

            for attempt in range(KAFKA_COMMIT_MAX_RETRIES):
                commit_started = time.perf_counter()
                try:
                    await self.consumer.commit(commit_offsets)
                    duration_ms = (time.perf_counter() - commit_started) * 1000
                    logger.info(
                        "kafka_commit_timing partition=%s next_offset=%s duration_ms=%.3f outcome=success",
                        item.topic_partition.partition, next_offset, duration_ms,
                    )
                    break
                except Exception:
                    duration_ms = (time.perf_counter() - commit_started) * 1000
                    logger.warning(
                        "kafka_commit_timing partition=%s next_offset=%s duration_ms=%.3f outcome=error",
                        item.topic_partition.partition, next_offset, duration_ms,
                    )
                    if attempt == KAFKA_COMMIT_MAX_RETRIES - 1:
                        logger.exception(
                            "kafka_offset_commit_failed "
                            "partition=%s next_offset=%s attempts=%s",
                            item.topic_partition.partition,
                            next_offset,
                            KAFKA_COMMIT_MAX_RETRIES,
                        )
                        raise
                    
                    delay = KAFKA_COMMIT_RETRY_DELAY * (2 ** attempt)

                    logger.warning(
                        "kafka_offset_commit_retry "
                        "partition=%s next_offset=%s attempt=%s delay_seconds=%s",
                        item.topic_partition.partition,
                        next_offset,
                        attempt + 1,
                        delay,
                    )
                    
                    await asyncio.sleep(delay)

            del records[:completed_count]

            if not records:
                del self.pending[item.topic_partition]


def create_work_queue() -> asyncio.Queue:
    """Create a bounded queue using centralized configuration"""
    return asyncio.Queue(maxsize = QUEUE_MAX_SIZE)

def deserialize_event(value: bytes) -> dict:
    """Decode JSON without allowing malformed payloads to crash deserialization"""
    if value is None:
        raise ValueError("Kafka message has no value")
    
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

    for attempt in range(DB_MAX_RETRIES):
        try:
            transaction_started = time.perf_counter()
            async with SessionLocal() as session:
                execute_started = time.perf_counter()
                await session.execute(statement)
                execute_ms = (time.perf_counter() - execute_started) * 1000
                commit_started = time.perf_counter()
                await session.commit()
                commit_ms = (time.perf_counter() - commit_started) * 1000
            total_ms = (time.perf_counter() - transaction_started) * 1000
            logger.info(
                "db_transaction_timing event_id=%s execute_ms=%.3f commit_ms=%.3f total_ms=%.3f",
                event_id, execute_ms, commit_ms, total_ms,
            )
            return
        except Exception:
            if attempt == DB_MAX_RETRIES - 1:
                logger.exception(
                    "event_persistence_failed event_id=%s attempts=%s",
                    event_id,
                    DB_MAX_RETRIES,
                )
                raise

            delay = min(
                DB_INITIAL_RETRY_DELAY * (2 ** attempt),
                DB_MAX_RETRY_DELAY,
            )
            logger.warning(
                "event_persistence_retry event_id=%s attempt=%s delay_seconds=%s",
                event_id,
                attempt + 1,
                delay,
            )
            await asyncio.sleep(delay)

async def process_event_batch(payloads: list[dict]) -> None:
    """Persist multiple events in one idempotent PostgreSQL transaction."""
    if not payloads:
        return

    values = []
    for payload in payloads:
        event_id = validate_event(payload)
        values.append({
            "id": event_id,
            "service": payload["service"],
            "level": payload["level"],
            "message": payload["message"],
            "attributes": payload.get("attributes", {}),
        })

    statement = (
        insert(Event)
        .values(values)
        .on_conflict_do_nothing(index_elements=[Event.id])
    )

    for attempt in range(DB_MAX_RETRIES):
        try:
            transaction_started = time.perf_counter()

            async with SessionLocal() as session:
                execute_started = time.perf_counter()
                await session.execute(statement)
                execute_ms = (time.perf_counter() - execute_started) * 1000

                commit_started = time.perf_counter()
                await session.commit()
                commit_ms = (time.perf_counter() - commit_started) * 1000

            total_ms = (time.perf_counter() - transaction_started) * 1000
            logger.info(
                "db_batch_timing count=%s execute_ms=%.3f "
                "commit_ms=%.3f total_ms=%.3f",
                len(values), execute_ms, commit_ms, total_ms,
            )
            return

        except Exception:
            if attempt == DB_MAX_RETRIES - 1:
                logger.exception(
                    "event_batch_persistence_failed count=%s attempts=%s",
                    len(values), DB_MAX_RETRIES,
                )
                raise

            delay = min(
                DB_INITIAL_RETRY_DELAY * (2 ** attempt),
                DB_MAX_RETRY_DELAY,
            )
            logger.warning(
                "event_batch_persistence_retry count=%s attempt=%s "
                "delay_seconds=%s",
                len(values), attempt + 1, delay,
            )
            await asyncio.sleep(delay)


async def _process_pending_batch(
    items: list[PendingRecord],
    worker_id: int,
    dead_letter_producer,
    offset_tracker: OffsetTracker | None,
) -> None:
    """Process a batch and mark records complete only after all work succeeds."""
    valid_items: list[tuple[PendingRecord, dict]] = []

    for item in items:
        try:
            if isinstance(item.payload, bytes) or item.payload is None:
                payload = deserialize_event(item.payload)
            elif isinstance(item.payload, dict):
                payload = item.payload
            else:
                raise ValueError("unsupported event payload type")

            validate_event(payload)
            valid_items.append((item, payload))

        except ValueError as exc:
            if dead_letter_producer is None:
                raise RuntimeError(
                    "DLQ producer is required for invalid records"
                ) from exc

            raw_val = item.payload
            if isinstance(raw_val, bytes):
                raw_val = raw_val.decode("utf-8", errors="replace")
            elif raw_val is not None and not isinstance(raw_val, str):
                raw_val = json.dumps(raw_val, default=str)

            dead_letter_payload = {
                "source_topic": item.topic,
                "source_partition": item.topic_partition.partition,
                "source_offset": item.offset,
                "error": str(exc),
                "raw_val": raw_val,
            }

            await dead_letter_producer.send_and_wait(
                KAFKA_DEAD_LETTER_TOPIC,
                json.dumps(dead_letter_payload).encode("utf-8"),
            )

            logger.warning(
                "event_sent_to_dlq worker_id=%s partition=%s offset=%s",
                worker_id,
                item.topic_partition.partition,
                item.offset,
            )

    if len(valid_items) == 1:
        # Preserve existing single-event behavior and compatibility with tests.
        await process_event(valid_items[0][1])
    elif len(valid_items) > 1:
        await process_event_batch(
            [payload for _, payload in valid_items]
        )

    for item, payload in valid_items:
        logger.info(
            "event_persisted worker_id=%s partition=%s offset=%s",
            worker_id,
            item.topic_partition.partition,
            item.offset,
        )

    # Do not advance any offsets until database persistence and all required
    # DLQ sends for this batch have succeeded.
    for item in items:
        if offset_tracker is not None:
            await offset_tracker.mark_completed(item)
        else:
            item.completed = True


async def persistence_worker(
    queue: asyncio.Queue,
    worker_id: int,
    dead_letter_producer=None,
    offset_tracker: OffsetTracker | None = None,
) -> None:
    """Persist queued events in small batches or route invalid events to DLQ."""
    batch_size = BATCH_SIZE
    batch_timeout = BATCH_TIMEOUT_SECONDS

    logger.info(
        "persistence worker started worker_id=%s batch_size=%s",
        worker_id,
        batch_size,
    )

    while True:
        first_item = await queue.get()
        items = [first_item]
        shutdown_seen = False

        try:
            # A sentinel received as the first item stops this worker.
            if first_item is None:
                return

            # Collect more records until the batch is full or the timeout expires.
            loop = asyncio.get_running_loop()
            deadline = loop.time() + batch_timeout

            while len(items) < batch_size:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                
                try:
                    candidate = await asyncio.wait_for(
                        queue.get(),
                        timeout = remaining,
                    )
                except asyncio.TimeoutError:
                    break

                if candidate is None:
                    queue.task_done()
                    shutdown_seen = True
                    break

                items.append(candidate)

            # Always process the collected batch, including a partial batch.
            await _process_pending_batch(
                items,
                worker_id,
                dead_letter_producer,
                offset_tracker,
            )

            if shutdown_seen:
                return

        except Exception:
            failed_item = items[0]
            logger.exception(
                "event_processing_failed worker_id=%s partition=%s offset=%s "
                "batch_size=%s",
                worker_id,
                failed_item.topic_partition.partition,
                failed_item.offset,
                len(items),
            )
            raise

        finally:
            # Each record removed from the queue is accounted for exactly once.
            for _ in items:
                queue.task_done()


async def supervise_workers(
    queue: asyncio.Queue,
    dead_letter_producer,
    offset_tracker: OffsetTracker,
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Monitor workers and support coordinated graceful shutdown."""
    
    if shutdown_event is None:
        shutdown_event = asyncio.Event()

    tasks = [
        asyncio.create_task(
            persistence_worker(
                queue, 
                worker_id = index+1,
                dead_letter_producer = dead_letter_producer,
                offset_tracker = offset_tracker,
                ),
            name = f"dislog-persistence-worker-{index+1}",
        )
        for index in range(WORKER_COUNT)
    ]

    shutdown_waiter = asyncio.create_task(shutdown_event.wait())

    try:
        done, _ = await asyncio.wait(
            [*tasks, shutdown_waiter],
            return_when = asyncio.FIRST_COMPLETED,
        )

        # Worker failures take precedence over a shutdown request.
        for task in tasks:
            if task.done():
                if task.cancelled():
                    raise RuntimeError(
                        f"Persistence worker was cancelled: {task.get_name()}"
                    )

                exception = task.exception()
                if exception is not None:
                    raise exception
                
                raise RuntimeError(
                    f"Persistence worker exited unexpectedly: {task.get_name()}"
                )
        
        if shutdown_waiter not in done:
            raise RuntimeError("Worker pool stopped unexpectedly")
        
        # Wait for queued and in-flight work to finish, while still
        # monitoring workers so a crashed worker cannot hang shutdown.
        join_task = asyncio.create_task(queue.join())

        try:
            done, _ = await asyncio.wait(
                [*tasks, join_task],
                return_when = asyncio.FIRST_COMPLETED,
            )

            for task in tasks:
                if task.done():
                    if task.cancelled():
                        raise RuntimeError(
                            f"Persistence worker was cancelled: {task.get_name()}"
                        )
                    
                    exception = task.exception()

                    if exception is not None:
                        raise exception
                    
                    raise RuntimeError(
                        f"Persistence worker exited unexpectedly: {task.get_name()}"
                    )

            if join_task not in done:
                raise RuntimeError("Worker exited before the queue drained")
            

            for _ in tasks:
                await queue.put(None)
            
            results = await asyncio.gather(
                *tasks,
                return_exceptions = True,
            )

            for task,result in zip(tasks, results):
                if isinstance(result, BaseException):
                    raise RuntimeError(
                        f"Worker failed during shutdown: {task.get_name()}"
                    ) from result
        
        finally:
            if not join_task.done():
                join_task.cancel()
                await asyncio.gather(join_task, return_exceptions=True)

    finally:
        shutdown_waiter.cancel()
        await asyncio.gather(shutdown_waiter, return_exceptions=True)

        for task in tasks:
            if not task.done():
                task.cancel()
        
        await asyncio.gather(*tasks, return_exceptions=True)

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

    queue = create_work_queue()
    offset_tracker = OffsetTracker(consumer)

    stop_consuming = asyncio.Event()
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    previous_signal_handlers = {}
    consumer_started = False
    producer_started = False

    worker_supervisor = None

    def request_shutdown() -> None:
        if not stop_consuming.is_set():
            logger.info("Shutdown requested")

        stop_consuming.set()
    
    def handle_signal(signum, frame) -> None:
        loop.call_soon_threadsafe(request_shutdown)
    

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous_signal_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig,handle_signal)
        except (ValueError, OSError):
            logger.debug(
                "Could not register handler for signal %s",
                sig,
            )
    
    try:
        await dead_letter_producer.start()
        producer_started = True

        await consumer.start()
        consumer_started = True

        logger.info("Kafka peristence worker started")

        # logger.info(
        #     "DisLog persistence worker started topic=%s workers=%s",
        #     KAFKA_TOPIC,
        #     WORKER_COUNT,
        # )

        worker_supervisor = asyncio.create_task(
            supervise_workers(
                queue,
                dead_letter_producer,
                offset_tracker,
                shutdown_event = shutdown_event,
            ),
            name = "dislog-worker-supervisor",
        )

        while not stop_consuming.is_set():
            # Fail promptly if a persistence worker has crashed.
            if worker_supervisor.done():
                await worker_supervisor
            
            batches = await consumer.getmany(
                timeout_ms = 1000,
                max_records = QUEUE_MAX_SIZE,
            )

            # A supervisor failure may have occurred while waiting for Kafka records.
            if worker_supervisor.done():
                await worker_supervisor

            # Finish enqueueing this fetched batch before initiating supervisor shutdown. 
            # These records remain eligible for processing and offset tracking.
            for records in batches.values():
                for record in records:
                    item = PendingRecord(
                        topic_partition = TopicPartition(
                            record.topic,
                            record.partition,
                        ),
                        offset = record.offset,
                        payload = record.value,
                        topic = record.topic,
                    )

                    offset_tracker.register(item)
                    await queue.put(item)
        
        logger.info("Kafka consumption stopped")

    finally:
        # No more records will be enqueued after this point.
        # The supervisor can now drain the queue and stop its workers.
        shutdown_event.set()
        try:
            if worker_supervisor is not None:
                await worker_supervisor
            
        finally:
            try:
                if consumer_started:
                    await consumer.stop()
                    logger.info("kafka consumer stopped")
            finally:
                try:
                    if producer_started:
                        await dead_letter_producer.stop()
                        logger.info("Dead-letter producer stopped")
                finally:
                    try:
                        await engine.dispose()
                        logger.info("Database engine disposed")
                    finally:
                        for sig, previous_handler in (previous_signal_handlers.items()):
                            try:
                                signal.signal(sig, previous_handler)
                            except(ValueError, OSError):
                                logger.debug(
                                    "Could not restore handler for signal %s",
                                    sig,
                                )


if __name__ == "__main__":
    asyncio.run(run_worker())

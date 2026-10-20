import asyncio
import statistics
import time
import uuid

from sqlalchemy import delete, select, func

import app.worker as worker
from app.database import SessionLocal, engine
from app.models import Event

EVENT_COUNT = 1000
BATCH_SIZES = [10,25,50,100]
RUN_ID = str(uuid.uuid4())


def make_payload():
    return {
        "id": str(uuid.uuid4()),
        "service": "dislog-benchmark",
        "level": "INFO",
        "message": f"Batch benchmark {RUN_ID}",
        "attributes": {"benchmark_run": RUN_ID},
    }


async def cleanup(event_ids):
    if not event_ids:
        return 0

    async with SessionLocal() as session:
        result = await session.execute(
            delete(Event).where(Event.id.in_(event_ids))
        )
        await session.commit()
        return result.rowcount or 0

async def count_records(event_ids):
    async with SessionLocal() as session:
        result = await session.execute(
            select(func.count())
            .select_from(Event)
            .where(Event.id.in_(event_ids))
        )
        return result.scalar_one()


async def benchmark_batch_size(batch_size):
    # Each configuration gets its own unique event IDs.
    payloads = [make_payload() for _ in range(EVENT_COUNT)]
    event_ids = [uuid.UUID(p["id"]) for p in payloads]

    try:
        start = time.perf_counter()

        for index in range(0, EVENT_COUNT, batch_size):
            batch = payloads[index:index + batch_size]
            await worker.process_event_batch(batch)

        elapsed = time.perf_counter() - start
        persisted = await count_records(event_ids)

        if persisted != EVENT_COUNT:
            raise RuntimeError(
                f"Batch size {batch_size}: expected {EVENT_COUNT} "
                f"records, found {persisted}"
            )

        return {
            "batch_size": batch_size,
            "seconds": elapsed,
            "throughput": EVENT_COUNT / elapsed,
            "transactions": (EVENT_COUNT + batch_size - 1) // batch_size,
            "verified": persisted,
        }
    finally:
        deleted = await cleanup(event_ids)
        print(
            f"Batch size {batch_size}: cleaned up "
            f"{deleted}/{EVENT_COUNT} benchmark records"
        )

async def main():
    # payloads = [make_payload() for _ in range(EVENT_COUNT)]
    # event_ids = [uuid.UUID(p["id"]) for p in payloads]

    # try:
    #     # Individual inserts: one function call per event.
    #     start = time.perf_counter()
    #     for payload in payloads:
    #         await worker.process_event(payload)
    #     individual_seconds = time.perf_counter() - start

    #     # Remove these IDs before the batch run so both runs insert new rows.
    #     await cleanup(event_ids)

    #     # Batch inserts: one function call per group of 25.
    #     start = time.perf_counter()
    #     for index in range(0, EVENT_COUNT, BATCH_SIZE):
    #         batch = payloads[index:index + BATCH_SIZE]
    #         await worker.process_event_batch(batch)
    #     batch_seconds = time.perf_counter() - start
    #     persisted_count = await count_benchmark_records(event_ids)
    #     print(f"Verified persisted records: {persisted_count}/{EVENT_COUNT}")

    #     if persisted_count != EVENT_COUNT:
    #         raise RuntimeError(
    #             f"Expected {EVENT_COUNT} records, found {persisted_count}"
    #         )

    #     print("\nBenchmark results")
    #     print(f"Events per run: {EVENT_COUNT}")
    #     print(f"Batch size: {BATCH_SIZE}")
    #     print(
    #         f"Individual inserts: {individual_seconds:.2f}s "
    #         f"({EVENT_COUNT / individual_seconds:.1f} events/s)"
    #     )
    #     print(
    #         f"Batch inserts:      {batch_seconds:.2f}s "
    #         f"({EVENT_COUNT / batch_seconds:.1f} events/s)"
    #     )
    #     print(
    #         f"Speedup: {individual_seconds / batch_seconds:.2f}x"
    #     )
    #     print(
    #         f"Expected transactions: {EVENT_COUNT} individual vs "
    #         f"{(EVENT_COUNT + BATCH_SIZE - 1) // BATCH_SIZE} batched"
    #     )

    # finally:
    #     # Remove only records generated for this benchmark.
    #     try:
    #         deleted_count = await cleanup(event_ids)
    #         print(
    #             f"Cleaned up {deleted_count} benchmark records "
    #             f"for run {RUN_ID}"
    #         )
    #     finally:
    #         await engine.dispose()

    results = []

    try:
        print(f"Benchmark run: {RUN_ID}")
        print(f"Events per configuration: {EVENT_COUNT}")

        for batch_size in BATCH_SIZES:
            print(f"\nTesting batch size {batch_size}...")
            result = await benchmark_batch_size(batch_size)
            results.append(result)

        baseline = next(
            result["seconds"]
            for result in results
            if result["batch_size"] == BATCH_SIZES[0]
        )

        print("\nBatch-size comparison")
        print("-" * 76)
        print(
            f"{'Size':>6} {'Time (s)':>12} {'Events/s':>12} "
            f"{'Transactions':>14} {'Speedup vs 10':>16}"
        )
        print("-" * 76)

        for result in results:
            speedup = baseline / result["seconds"]
            print(
                f"{result['batch_size']:>6} "
                f"{result['seconds']:>12.2f} "
                f"{result['throughput']:>12.1f} "
                f"{result['transactions']:>14} "
                f"{speedup:>15.2f}x"
            )

        best = max(results, key=lambda result: result["throughput"])
        print(
            f"\nHighest measured throughput: batch size "
            f"{best['batch_size']} ({best['throughput']:.1f} events/s)"
        )
        print(
            "This is the fastest single run, not necessarily the "
            "best production setting; repeat runs before choosing."
        )

    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())

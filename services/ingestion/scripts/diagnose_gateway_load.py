import asyncio
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

from sqlalchemy import select

from app.database import SessionLocal, engine
from app.models import Event

GATEWAY_URL = "http://127.0.0.1:3000/api/v1/events"
BATCH_SIZE = 1000
CONCURRENCY = 25
PERSISTENCE_TIMEOUT = 180


def post_event(index, run_id):
    payload = {
        "service": "dislog-diagnostic",
        "level": "INFO",
        "message": f"Persistence diagnostic event {index}",
        "attributes": {
            "test_run": run_id,
            "sequence": index,
        },
    }

    request = Request(
        GATEWAY_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-request-id": f"dislog-diagnostic-{run_id}-{index}",
        },
        method="POST",
    )

    started = time.perf_counter()

    try:
        with urlopen(request, timeout=15) as response:
            status = response.status
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        status = exc.code
        body = exc.read().decode("utf-8", errors="replace")
    except (URLError, TimeoutError, OSError) as exc:
        return {
            "index": index,
            "status": 0,
            "event_id": None,
            "latency": time.perf_counter() - started,
            "error": str(exc),
        }

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {}

    raw_id = data.get("id") or data.get("event_id")
    try:
        event_id = str(UUID(str(raw_id))) if raw_id else None
    except (ValueError, TypeError, AttributeError):
        event_id = None

    return {
        "index": index,
        "status": status,
        "event_id": event_id,
        "latency": time.perf_counter() - started,
        "error": None if event_id else body[:300],
    }


async def main():
    run_id = str(uuid4())
    started = time.perf_counter()
    accepted_ids = set()

    print(f"Run ID: {run_id}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Concurrency: {CONCURRENCY}")
    print(f"Gateway: {GATEWAY_URL}")

    try:
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            results = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: list(
                    executor.map(
                        lambda i: post_event(i, run_id),
                        range(BATCH_SIZE),
                    )
                ),
            )

        successful = [
            r for r in results
            if 200 <= r["status"] < 300
        ]
        accepted_ids = {
            r["event_id"] for r in successful if r["event_id"]
        }
        latencies = [r["latency"] for r in results]
        ordered = sorted(latencies)

        print("\n--- HTTP results ---")
        print(f"Requests: {len(results)}")
        print(f"Successful HTTP responses: {len(successful)}")
        print(f"Responses with event IDs: {len(accepted_ids)}")
        print(f"HTTP duration: {time.perf_counter() - started:.3f}s")
        print(f"Mean latency: {statistics.mean(latencies) * 1000:.1f} ms")
        print(f"Median latency: {statistics.median(latencies) * 1000:.1f} ms")
        print(
            f"p95 latency: "
            f"{ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))] * 1000:.1f} ms"
        )

        for result in results:
            if not (200 <= result["status"] < 300) or not result["event_id"]:
                print(
                    f"HTTP issue: request={result['index']} "
                    f"status={result['status']} error={result['error']}"
                )

        if not accepted_ids:
            print("No usable event IDs; cannot verify persistence.")
            return

        print("\n--- Waiting for PostgreSQL persistence ---")
        print(f"Accepted IDs: {len(accepted_ids)}")
        deadline = time.perf_counter() + PERSISTENCE_TIMEOUT
        uuid_ids = [UUID(value) for value in accepted_ids]
        persisted_ids = set()

        while True:
            async with SessionLocal() as session:
                result = await session.execute(
                    select(Event.id).where(Event.id.in_(uuid_ids))
                )
                persisted_ids = {
                    str(value) for value in result.scalars().all()
                }

            print(
                f"\rPersisted: {len(persisted_ids)}/{len(accepted_ids)}",
                end="",
                flush=True,
            )

            if persisted_ids == accepted_ids:
                break
            if time.perf_counter() >= deadline:
                break

            await asyncio.sleep(0.5)

        missing_ids = accepted_ids - persisted_ids
        elapsed = time.perf_counter() - started

        print("\n\n--- Persistence results ---")
        print(f"Accepted IDs: {len(accepted_ids)}")
        print(f"Persisted IDs: {len(persisted_ids)}")
        print(f"Missing IDs: {len(missing_ids)}")
        print(f"Total elapsed: {elapsed:.3f}s")
        print(
            f"Verified throughput: {len(persisted_ids) / elapsed:.2f} events/sec"
        )

        if missing_ids:
            print("\nMissing event IDs (records are NOT deleted):")
            for event_id in sorted(missing_ids):
                print(event_id)

            print("\nRESULT: FAIL — missing records preserved for investigation.")
        elif len(accepted_ids) == BATCH_SIZE and len(successful) == BATCH_SIZE:
            print(f"\nRESULT: PASS — all {len(accepted_ids)} events were accepted and persisted.")
        else:
            print("\nRESULT: PARTIAL — inspect HTTP failures above.")

        print(f"\nDiagnostic run ID: {run_id}")
        print("Cleanup: none; all test records are preserved.")

    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

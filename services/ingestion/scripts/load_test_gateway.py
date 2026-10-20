import argparse
import asyncio
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

from sqlalchemy import delete, select

from app.database import SessionLocal
from app.models import Event


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="DisLog gateway end-to-end load test"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=500,
        help="Number of events to send (default: 500)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=25,
        help="HTTP concurrency / thread count (default: 25)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Timeout in seconds for persistence verification (default: 60)",
    )
    parser.add_argument(
        "--gateway-url",
        type=str,
        default="http://127.0.0.1:3000/api/v1/events",
        help="Gateway events URL",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Keep persisted records in the database (diagnostic mode)",
    )
    return parser.parse_args(args)


def post_event(index, run_id, gateway_url):
    payload = {
        "service": "dislog-load-test",
        "level": "INFO",
        "message": f"End-to-end load test event {index}",
        "attributes": {
            "test_run": run_id,
            "sequence": index,
        },
    }

    request = Request(
        gateway_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-request-id": f"dislog-load-{run_id}-{index}",
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

    latency = time.perf_counter() - started

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {}

    event_id = data.get("event_id") or data.get("id")

    # Accept only responses that include a usable event ID.
    try:
        event_id = str(UUID(str(event_id))) if event_id else None
    except (ValueError, TypeError, AttributeError):
        event_id = None

    return {
        "index": index,
        "status": status,
        "event_id": event_id,
        "latency": latency,
        "error": None if event_id else body[:300],
    }


def run_batch(run_id, count, concurrency, gateway_url):
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        return list(
            executor.map(
                lambda index: post_event(index, run_id, gateway_url),
                range(count),
            )
        )


async def main(args=None):
    if args is None:
        args = parse_args()

    run_id = str(uuid4())
    accepted_ids = set()
    results = []
    persistence_seconds = None
    persisted_ids = set()

    batch_started = time.perf_counter()

    try:
        print(f"Run ID: {run_id}")
        print(f"Sending {args.count} events via {args.gateway_url}")
        print(f"HTTP concurrency: {args.concurrency}")

        results = await asyncio.to_thread(
            run_batch, run_id, args.count, args.concurrency, args.gateway_url
        )

        batch_finished = time.perf_counter()

        accepted_ids = {
            result["event_id"]
            for result in results
            if 200 <= result["status"] < 300
            and result["event_id"] is not None
        }

        latencies = [result["latency"] for result in results]
        successful_http = sum(
            200 <= result["status"] < 300 for result in results
        )
        request_errors = [
            result for result in results
            if not (200 <= result["status"] < 300)
            or result["event_id"] is None
        ]

        print("\n--- HTTP results ---")
        print(f"Requests attempted: {len(results)}")
        print(f"Successful HTTP responses: {successful_http}")
        print(f"Responses with usable event IDs: {len(accepted_ids)}")
        print(f"Acceptance rate: {successful_http / args.count * 100:.2f}%")
        print(f"HTTP batch duration: {batch_finished - batch_started:.3f}s")
        print(f"Mean request latency: {statistics.mean(latencies) * 1000:.1f} ms")
        print(f"Median request latency: {statistics.median(latencies) * 1000:.1f} ms")
        print(
            "p95 request latency: "
            f"{sorted(latencies)[min(len(latencies) - 1, int(len(latencies) * 0.95))] * 1000:.1f} ms"
        )

        if request_errors:
            print(f"Requests needing investigation: {len(request_errors)}")
            for item in request_errors[:5]:
                print(
                    f"  Request {item['index']}: "
                    f"HTTP {item['status']}; {item['error']}"
                )

        if not accepted_ids:
            print("\nNo usable event IDs were returned; persistence cannot be verified.")
            return

        # Poll PostgreSQL through the SQLAlchemy async engine.
        persistence_started = time.perf_counter()
        deadline = persistence_started + args.timeout
        uuid_ids = [UUID(event_id) for event_id in accepted_ids]

        while True:
            async with SessionLocal() as session:
                result = await session.execute(
                    select(Event.id).where(Event.id.in_(uuid_ids))
                )
                persisted_ids = {
                    str(event_id) for event_id in result.scalars().all()
                }

            if persisted_ids == accepted_ids:
                break

            if time.perf_counter() >= deadline:
                break

            await asyncio.sleep(0.5)

        persistence_seconds = time.perf_counter() - persistence_started
        total_seconds = time.perf_counter() - batch_started
        missing_ids = accepted_ids - persisted_ids

        print("\n--- Persistence results ---")
        print(f"Accepted event IDs: {len(accepted_ids)}")
        print(f"Persisted event IDs: {len(persisted_ids)}")
        print(f"Missing event IDs: {len(missing_ids)}")
        print(f"Time to persist accepted events: {persistence_seconds:.3f}s")
        print(f"End-to-end duration: {total_seconds:.3f}s")
        print(
            "End-to-end throughput: "
            f"{len(persisted_ids) / total_seconds:.2f} persisted events/sec"
        )

        if missing_ids:
            print("First missing IDs:")
            for event_id in sorted(missing_ids)[:10]:
                print(f"  {event_id}")

        if (
            len(results) == args.count
            and successful_http == args.count
            and len(accepted_ids) == args.count
            and not missing_ids
        ):
            print("\nRESULT: PASS — all events were accepted and persisted.")
        else:
            print("\nRESULT: FAIL — investigate HTTP errors or missing records.")

    finally:
        if accepted_ids and not args.no_cleanup:
            async with SessionLocal() as session:
                await session.execute(
                    delete(Event).where(
                        Event.id.in_([UUID(event_id) for event_id in accepted_ids])
                    )
                )
                await session.commit()

            print(f"\nCleaned up {len(accepted_ids)} test event IDs.")
        elif accepted_ids and args.no_cleanup:
            print(f"\nDiagnostic mode: preserved {len(accepted_ids)} records in database.")

        await SessionLocal.kw["bind"].dispose()


if __name__ == "__main__":
    asyncio.run(main())

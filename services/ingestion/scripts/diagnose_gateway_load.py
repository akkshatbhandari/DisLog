"""Diagnostic load test runner that runs 1000 events and preserves records for inspection."""
import asyncio
from scripts.load_test_gateway import main, parse_args

if __name__ == "__main__":
    args = parse_args(["--count", "1000", "--timeout", "180", "--no-cleanup"])
    asyncio.run(main(args))

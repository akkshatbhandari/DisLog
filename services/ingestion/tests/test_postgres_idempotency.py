import pytest
from sqlalchemy import func, select, delete

import app.worker as worker
from app.database import SessionLocal, engine
from app.models import Event
from tests.test_worker import valid_payload

@pytest.mark.asyncio
async def test_postgres_persists_redelivered_event_once():
    payload = valid_payload()
    event_id = worker.validate_event(payload)

    try:
        # Process the same event twice with the same UUID.
        await worker.process_event(payload)
        await worker.process_event(payload)

        async with SessionLocal() as session:
            result = await session.execute(
                select(func.count())
                .select_from(Event)
                .where(Event.id == event_id)
            )

            row_count = result.scalar_one()
        
        assert row_count == 1
    
    finally:
        # Remove only this test's event.
        async with SessionLocal() as session:
            await session.execute(
                delete(Event).where(Event.id == event_id)
            )
            await session.commit()
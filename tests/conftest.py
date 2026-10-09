import os

import pytest

from jobqueue.registry import Registry
from jobqueue.store import Store

TEST_DSN = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://postgres@localhost:5432/jobqueue_test"
)


@pytest.fixture
def store():
    s = Store(TEST_DSN)
    s.migrate()
    s.truncate_all()
    yield s
    s.close()


@pytest.fixture
def registry():
    return Registry()


def expire_lease(store, job_id):
    """Simulate time passing until a running job's lease is over."""
    with store.pool.connection() as conn:
        conn.execute(
            "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = %s",
            (job_id,),
        )


def make_runnable_now(store, job_id):
    """Simulate time passing until a delayed or backed-off job is due."""
    with store.pool.connection() as conn:
        conn.execute("UPDATE jobs SET run_at = now() WHERE id = %s", (job_id,))

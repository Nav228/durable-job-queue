"""Worker: claim a job, run its handler under a heartbeated lease, report the outcome.

Run a pool:  python -m jobqueue.worker --concurrency 4 --queue default
"""
from __future__ import annotations

import argparse
import logging
import os
import random
import signal
import socket
import threading
import time
import traceback
from typing import Any, Callable

from .errors import PermanentError
from .registry import Registry
from .store import Job, Store

log = logging.getLogger("jobqueue.worker")


def exponential_backoff(attempt: int, base: float = 2.0, cap: float = 300.0) -> float:
    """2s, 4s, 8s ... capped, with full jitter so a burst of failures
    doesn't retry in lockstep and hammer the same dependency."""
    return random.uniform(0, min(cap, base * 2 ** (attempt - 1)))


class JobContext:
    """What a handler gets besides its payload."""

    def __init__(self, store: Store, job: Job, lease_lost: threading.Event):
        self._store = store
        self.job = job
        self._lease_lost = lease_lost

    @property
    def attempt(self) -> int:
        return self.job.attempts

    @property
    def lease_lost(self) -> bool:
        """True if another worker may now own this job. Long handlers should
        check this between steps and stop early."""
        return self._lease_lost.is_set()

    def once(self, name: str, fn: Callable[[], Any]) -> Any:
        """Run a side effect at most once per job across retries.

        If a previous attempt already recorded `name`, return its stored result
        instead of running `fn` again. Caveat: if the process dies after `fn`
        runs but before the record is written, the effect can repeat. For
        external APIs, also pass an idempotency key derived from
        `effect_key(name)` so the downstream system dedupes that window too.
        """
        found, result = self._store.get_effect(self.job.id, name)
        if found:
            return result
        result = fn()
        self._store.record_effect(self.job.id, name, result)
        return result

    def effect_key(self, name: str) -> str:
        """Stable key to hand to downstream APIs (e.g. a Stripe Idempotency-Key)."""
        return f"job-{self.job.id}-{name}"


class _Heartbeat(threading.Thread):
    def __init__(self, store: Store, job: Job, lease_seconds: float, lost: threading.Event):
        super().__init__(daemon=True, name=f"heartbeat-{job.id}")
        self.store, self.job, self.lease_seconds, self.lost = store, job, lease_seconds, lost
        self.done = threading.Event()

    def run(self) -> None:
        interval = self.lease_seconds / 3  # survive two missed beats
        while not self.done.wait(interval):
            try:
                if not self.store.heartbeat(self.job, self.lease_seconds):
                    log.warning("job %s: lease lost", self.job.id)
                    self.lost.set()
                    return
            except Exception:  # transient DB blip: try again next beat
                log.exception("job %s: heartbeat error", self.job.id)


class Worker:
    def __init__(
        self,
        store: Store,
        registry: Registry,
        worker_id: str | None = None,
        queues: tuple[str, ...] = ("default",),
        lease_seconds: float = 30,
        poll_interval: float = 0.5,
        backoff: Callable[[int], float] = exponential_backoff,
        reap_every: float = 5.0,
    ):
        self.store = store
        self.registry = registry
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{threading.get_ident()}"
        self.queues = queues
        self.lease_seconds = lease_seconds
        self.poll_interval = poll_interval
        self.backoff = backoff
        self.reap_every = reap_every
        self._stop = threading.Event()

    def stop(self) -> None:
        """Ask the worker to exit after its current job. Never interrupts a job."""
        self._stop.set()

    def run_forever(self) -> None:
        log.info("worker %s started on %s", self.worker_id, self.queues)
        next_reap = 0.0
        while not self._stop.is_set():
            # Wall-clock timer, so a worker that is never idle still reaps.
            if time.monotonic() >= next_reap:
                try:
                    n = self.store.reap_expired()
                    if n:
                        log.warning("reaped %d expired job(s)", n)
                except Exception:
                    log.exception("reaper error")
                next_reap = time.monotonic() + self.reap_every
            try:
                worked = self.run_once()
            except Exception:
                log.exception("worker loop error")
                worked = False
            if not worked:
                self._stop.wait(self.poll_interval)
        log.info("worker %s stopped", self.worker_id)

    def run_once(self) -> bool:
        """Claim and process one job. Returns False if nothing was due."""
        if self._stop.is_set():
            return False
        job = self.store.claim(self.worker_id, self.queues, self.lease_seconds)
        if job is None and self.store.reap_expired():
            # Idle and something was stuck: recover it right away instead of
            # waiting for the periodic reap. Costs nothing on the busy path.
            job = self.store.claim(self.worker_id, self.queues, self.lease_seconds)
        if job is None:
            return False

        lost = threading.Event()
        beat = _Heartbeat(self.store, job, self.lease_seconds, lost)
        beat.start()
        try:
            self._execute(job, lost)
        finally:
            beat.done.set()
        return True

    def _execute(self, job: Job, lost: threading.Event) -> None:
        handler = self.registry.get(job.type)
        if handler is None:
            self.store.fail(job, f"no handler registered for type {job.type!r}", 0, permanent=True)
            return

        ctx = JobContext(self.store, job, lost)
        try:
            result = handler(job.payload, ctx)
        except PermanentError as e:
            self._report(self.store.fail(job, _fmt(e), 0, permanent=True), job, "dead")
        except Exception as e:
            delay = self.backoff(job.attempts)
            self._report(self.store.fail(job, _fmt(e), delay), job, f"failed, retry in {delay:.1f}s")
        else:
            self._report(self.store.complete(job, result), job, "succeeded")

    @staticmethod
    def _report(accepted: bool, job: Job, outcome: str) -> None:
        if accepted:
            log.info("job %s (%s) attempt %d: %s", job.id, job.type, job.attempts, outcome)
        else:
            # Our lease expired and someone else owns the job now. Their result wins.
            log.warning("job %s: lost lease before reporting %r; result discarded", job.id, outcome)


def _fmt(e: BaseException) -> str:
    tb = traceback.format_exception(type(e), e, e.__traceback__, limit=5)
    return f"{type(e).__name__}: {e}\n{''.join(tb)}"[:4000]


def main() -> None:
    from .handlers import registry

    p = argparse.ArgumentParser(description="Run a pool of job queue workers.")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--queue", action="append", dest="queues")
    p.add_argument("--lease-seconds", type=float, default=30)
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(threadName)s %(message)s")
    store = Store(os.environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/jobqueue"),
                  max_size=args.concurrency * 2 + 2)
    store.migrate()

    host = f"{socket.gethostname()}-{os.getpid()}"
    workers = [
        Worker(store, registry, worker_id=f"{host}-w{i}", queues=tuple(args.queues or ["default"]),
               lease_seconds=args.lease_seconds)
        for i in range(args.concurrency)
    ]
    threads = [threading.Thread(target=w.run_forever, name=w.worker_id) for w in workers]

    def shutdown(signum, _frame):
        log.info("signal %s: finishing in-flight jobs, then exiting", signum)
        for w in workers:
            w.stop()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    store.close()


if __name__ == "__main__":
    main()

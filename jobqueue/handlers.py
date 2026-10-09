"""Example handlers. Add your own with @registry.handler("name")."""
import random
import time

from .errors import PermanentError
from .registry import Registry

registry = Registry()


@registry.handler("echo")
def echo(payload, ctx):
    return payload


@registry.handler("sleep")
def sleep(payload, ctx):
    """Simulates slow work in steps, bailing out if the lease is lost."""
    seconds = float(payload.get("seconds", 1))
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if ctx.lease_lost:
            raise RuntimeError("lease lost, stopping early")
        time.sleep(min(0.2, end - time.monotonic()))
    return {"slept": seconds}


@registry.handler("flaky")
def flaky(payload, ctx):
    """Fails with probability `fail_rate`. Exercises retries and backoff."""
    if random.random() < float(payload.get("fail_rate", 0.5)):
        raise RuntimeError(f"random failure on attempt {ctx.attempt}")
    return {"succeeded_on_attempt": ctx.attempt}


@registry.handler("send_email")
def send_email(payload, ctx):
    """The pattern for a non-repeatable side effect: validate, then do the
    effect through ctx.once so a retry after a crash doesn't send twice."""
    to = payload.get("to", "")
    if "@" not in to:
        raise PermanentError(f"invalid recipient {to!r}")

    def deliver():
        # A real implementation would call the email provider here, passing
        # ctx.effect_key("deliver") as its idempotency key.
        return {"message_id": f"msg-{ctx.job.id}", "to": to}

    return ctx.once("deliver", deliver)

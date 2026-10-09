class PermanentError(Exception):
    """Raise from a handler when retrying cannot help (bad input, 4xx from an API).
    The job goes straight to dead instead of burning its retry budget."""


class IdempotencyConflict(Exception):
    """Same idempotency key was reused for a different job."""


class JobNotFound(Exception):
    pass


class InvalidJobState(Exception):
    pass

from typing import Any, Callable

Handler = Callable[[Any, "JobContext"], Any]  # noqa: F821


class Registry:
    """Maps job type names to handler functions: handler(payload, ctx) -> result."""

    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def handler(self, name: str) -> Callable[[Handler], Handler]:
        def register(fn: Handler) -> Handler:
            if name in self._handlers:
                raise ValueError(f"handler {name!r} already registered")
            self._handlers[name] = fn
            return fn

        return register

    def get(self, name: str) -> Handler | None:
        return self._handlers.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self._handlers

    def names(self) -> list[str]:
        return sorted(self._handlers)

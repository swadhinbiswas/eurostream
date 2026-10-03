"""Structured logging: one format, correlated, and safe to ship to a log drain.

Three pieces:

* :func:`configure_logging` installs a single formatter (``text`` for humans,
  ``json`` for aggregation) instead of the ``logging.basicConfig`` call that
  used to live at import time in ``api``/``cli`` — importing a module must not
  mutate global logging state for the host process.
* A request-id :class:`~contextvars.ContextVar` is bound by the API middleware
  and echoed into every record emitted while that request is in scope, so a
  line from the erasure cascade can be joined against the HTTP access log and
  the ``X-Request-ID`` response header.
* Secret-looking keys (``password``, ``token``, ``salt``, ...) are redacted
  before serialisation, because configuration errors are the usual way
  credentials end up in a log drain.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from contextvars import ContextVar, Token
from typing import IO, Any, Literal

LogFormat = Literal["text", "json"]

#: Correlation id of the request currently being served (``None`` outside one).
REQUEST_ID: ContextVar[str | None] = ContextVar("eurostream_request_id", default=None)

REDACTED = "***"

# Attributes the stdlib owns; anything else on the record is caller-supplied
# context (``extra=...``) and belongs in the structured payload.
_RESERVED = frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "message",
    "asctime",
}

_SECRET_MARKERS = (
    "password",
    "passwd",
    "token",
    "secret",
    "authorization",
    "cookie",
    "salt",
    "api_key",
    "apikey",
    "credential",
)


def get_request_id() -> str | None:
    """Correlation id for the request in scope, if any."""
    return REQUEST_ID.get()


def set_request_id(value: str) -> Token[str | None]:
    """Bind ``value`` for the current context; keep the token to undo it."""
    return REQUEST_ID.set(value)


def reset_request_id(token: Token[str | None]) -> None:
    """Restore the previous binding (or ``None``) after a request finishes."""
    REQUEST_ID.reset(token)


def is_secret_key(key: str) -> bool:
    """Whether a payload key names something that must never be logged."""
    lowered = key.lower()
    return any(marker in lowered for marker in _SECRET_MARKERS)


class JsonFormatter(logging.Formatter):
    """One JSON object per line: stable keys, request id, redacted secrets."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        request_id = record.__dict__.get("request_id") or get_request_id()
        if request_id:
            payload["request_id"] = request_id
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            payload[key] = REDACTED if is_secret_key(key) else value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)
        # ``default=str`` so Paths and enums serialise instead of raising:
        # a logging failure inside a handler must never take a request down.
        return json.dumps(payload, default=str)


class ContextTextFormatter(logging.Formatter):
    """Human-readable line that carries the in-scope request id if there is one."""

    def format(self, record: logging.LogRecord) -> str:
        record.__dict__.setdefault("request_id", get_request_id() or "-")
        return super().format(record)


def build_formatter(fmt: LogFormat) -> logging.Formatter:
    if fmt == "json":
        return JsonFormatter()
    return ContextTextFormatter(
        "%(asctime)s %(levelname)-7s %(name)s [%(request_id)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def coerce_level(level: str) -> int:
    value = getattr(logging, str(level).upper(), None)
    if not isinstance(value, int):
        raise ValueError(f"invalid log level: {level!r}")
    return value


# Handlers this module installed, so re-configuring (CLI start-up, then an app
# built from settings, then a test) replaces ours instead of stacking copies.
_INSTALLED: list[logging.Handler] = []


def installed_handlers() -> list[logging.Handler]:
    """Handlers this module currently has on the root logger."""
    return list(_INSTALLED)


def configure_logging(
    level: str = "INFO",
    fmt: LogFormat = "text",
    *,
    stream: IO[str] | None = None,
) -> logging.Logger:
    """Install the EuroStream handler on the root logger. Idempotent.

    Handlers are tracked rather than matched by class so we never remove a
    handler somebody else (pytest's capture, a host application) installed.
    """
    root = logging.getLogger()
    for handler in _INSTALLED:
        root.removeHandler(handler)
        handler.close()
    _INSTALLED.clear()

    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(build_formatter(fmt))
    root.addHandler(handler)
    _INSTALLED.append(handler)
    root.setLevel(coerce_level(level))
    return root

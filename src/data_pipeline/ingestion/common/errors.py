"""Errors shared by the source adapters."""

from __future__ import annotations


class SourceResponseError(RuntimeError):
    """Raised when the upstream response cannot form a trustworthy raw extract."""


class IntradayRetentionError(SourceResponseError):
    """Raised when an intraday window predates what the source still retains.

    Both sources answer a too-old intraday request with an empty but otherwise
    successful payload, which looks exactly like a public holiday. Raising here
    keeps an out-of-retention partition from landing a silently empty batch.
    """


class IntradayGrainError(SourceResponseError):
    """Raised when a source answers an intraday request with daily bars.

    SSI accepts any `resolution` value with HTTP 200 and quietly serves daily
    candles for the ones it does not recognise, so the grain of what came back
    is verified rather than assumed.
    """

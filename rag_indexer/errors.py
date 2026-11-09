class TransientError(Exception):
    """Retryable failure. Consumer will route to the appropriate retry delay queue."""


class FatalError(Exception):
    """Non-retryable failure. Consumer will route directly to DLQ."""


class UnsupportedError(FatalError):
    """openRAG cannot index this format. Consumer acks it and reports "notsupported"."""

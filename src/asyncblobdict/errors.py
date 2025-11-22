class ConcurrencyError(Exception):
    """Raised when ETag optimistic concurrency check fails."""

    pass


class BlobNotFoundError(Exception):
    """Raised when a requested blob does not exist."""

    pass

"""
asyncblobdict
=============

Async-friendly key–value store for JSON and binary data, backed by local filesystem or Azure Blob Storage.

Main entry points:
- AsyncBlobStore: the store class
- CacheMode, ConcurrencyMode: enums for caching and concurrency
- LocalFileAdapter, AzureBlobAdapter: storage backends
- BlobNotFoundError, ConcurrencyError: exceptions
- SerializerView: format-specific view (e.g., .json, .binary)

Example:
    from asyncblobdict import AsyncBlobStore, LocalFileAdapter, CacheMode, ConcurrencyMode

    store = AsyncBlobStore(
        LocalFileAdapter("./data"),
        "container",
        cache_mode=CacheMode.WRITE_THROUGH,
        concurrency_mode=ConcurrencyMode.ETAG,
    )
"""

from .async_blob_store import (
    AsyncBlobStore,
    CacheMode,
    ConcurrencyMode,
    BlobNotFoundError,
    ConcurrencyError,
    SerializerView,
)

from .serializers import Serializer, JSONSerializer, BinarySerializer

from .storage_protocols import (
    AsyncStorageAdapter,
    AsyncContainerHandle,
    AsyncBlobHandle,
)
from .local_file_adapter import LocalFileAdapter
from .azure_blob_adapter import AzureBlobAdapter

import importlib.metadata

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "AsyncBlobStore",
    "CacheMode",
    "ConcurrencyMode",
    "BlobNotFoundError",
    "ConcurrencyError",
    "SerializerView",
    "Serializer",
    "JSONSerializer",
    "BinarySerializer",
    "AsyncStorageAdapter",
    "AsyncContainerHandle",
    "AsyncBlobHandle",
    "LocalFileAdapter",
    "AzureBlobAdapter",
]

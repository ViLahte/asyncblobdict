from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal

from .errors import BlobNotFoundError, ConcurrencyError
from .serializers import BinarySerializer, JSONSerializer, Serializer
from .storage_protocols import AsyncBlobHandle, AsyncStorageAdapter


class CacheMode(Enum):
    NONE = "none"  # No caching at all
    WRITE_THROUGH = "write_through"  # Cache + write immediately to backend
    WRITE_BACK = "write_back"  # Cache + sync later


class ConcurrencyMode(Enum):
    NONE = "none"  # No concurrency control
    ETAG = "etag"  # Use ETag optimistic concurrency


@dataclass
class CacheEntry:
    value: bytes
    etag: str | None


class AsyncBlobStoreCore:
    """
    Core store: raw bytes only.
    Handles caching and concurrency control.
    Format-agnostic — does not know about JSON/Binary extensions.
    """

    def __init__(
        self,
        adapter: AsyncStorageAdapter,
        container_name: str,
        cache_mode: CacheMode = CacheMode.NONE,
        concurrency_mode: ConcurrencyMode = ConcurrencyMode.NONE,
    ) -> None:
        self.adapter = adapter
        self.container = adapter.get_container(container_name)
        self.cache_mode = cache_mode
        self.concurrency_mode = concurrency_mode
        self._cache: dict[str, CacheEntry] = {}

    async def __aenter__(self) -> "AsyncBlobStoreCore":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        await self.adapter.close()

    @staticmethod
    async def get_etag_or_none(blob: AsyncBlobHandle) -> str | None:
        try:
            return await blob.get_etag()
        except BlobNotFoundError:
            return None

    async def get_bytes(self, blob_name: str) -> bytes:
        if self.cache_mode != CacheMode.NONE and blob_name in self._cache:
            return self._cache[blob_name].value

        blob = self.container.get_blob(blob_name)
        data = await blob.download()
        etag = (
            await blob.get_etag()
            if self.concurrency_mode == ConcurrencyMode.ETAG
            else None
        )

        if self.cache_mode != CacheMode.NONE:
            self._cache[blob_name] = CacheEntry(value=data, etag=etag)

        return data

    async def set_bytes(
        self,
        blob_name: str,
        data: bytes,
        etag: str | None = None,
    ) -> None:
        if self.cache_mode != CacheMode.NONE:
            self._cache[blob_name] = CacheEntry(value=data, etag=etag)

        if self.cache_mode in (CacheMode.WRITE_THROUGH, CacheMode.NONE):
            if self.concurrency_mode == ConcurrencyMode.ETAG:
                if etag is None:
                    blob = self.container.get_blob(blob_name)
                    etag = await self.get_etag_or_none(blob)

            blob = self.container.get_blob(blob_name)
            await blob.upload(data, overwrite=True, if_match=etag)

    async def delete(self, blob_name: str) -> None:
        blob = self.container.get_blob(blob_name)
        try:
            await blob.delete()
        except BlobNotFoundError:
            pass

        if blob_name in self._cache:
            del self._cache[blob_name]

    async def list_keys(self, prefix: str = "") -> list[str]:
        return await self.container.list_blob_names(prefix)

    async def sync(
        self, etag_behavior: Literal["skip", "overwrite", "raise"] = "skip"
    ) -> None:
        if self.cache_mode == CacheMode.NONE:
            raise RuntimeError("Cache is disabled, nothing to sync.")

        for blob_name, entry in list(self._cache.items()):
            blob = self.container.get_blob(blob_name)
            latest_etag = await self.get_etag_or_none(blob)

            if (
                self.concurrency_mode == ConcurrencyMode.ETAG
                and entry.etag is not None
                and latest_etag != entry.etag
            ):
                if etag_behavior == "skip":
                    print(
                        f"[Sync] [WARNING] Conflict detected for '{blob_name}', skipping upload."
                    )
                    continue
                elif etag_behavior == "raise":
                    raise ConcurrencyError(
                        f"ETag mismatch for '{blob_name}': cached={entry.etag}, remote={latest_etag}"
                    )
                elif etag_behavior == "overwrite":
                    print(
                        f"[Sync] [INFO] Conflict detected for '{blob_name}', overwriting."
                    )
                    latest_etag = None

            await blob.upload(entry.value, overwrite=True, if_match=latest_etag)
            self._cache[blob_name] = CacheEntry(
                value=entry.value, etag=await self.get_etag_or_none(blob)
            )


class SerializerView:
    """A namespaced view into AsyncBlobStore for a specific serializer format."""

    def __init__(self, store: "AsyncBlobStore", format: str):
        self._store = store
        self._format = format

    async def get(self, key: str):
        return await self._store._get(key, self._format)

    async def set(self, key: str, value: Any):
        return await self._store._set(key, value, self._format)

    async def delete(self, key: str):
        return await self._store._delete(key, self._format)


class AsyncBlobStore:
    json: SerializerView
    binary: SerializerView
    default_view: SerializerView

    """
    Format-aware store: wraps AsyncBlobStoreCore to add serializer-driven naming.
    Provides serializer-specific views (e.g., .json, .binary) and a default view.
    """

    def __init__(
        self,
        adapter: AsyncStorageAdapter,
        container_name: str,
        cache_mode: CacheMode = CacheMode.NONE,
        concurrency_mode: ConcurrencyMode = ConcurrencyMode.NONE,
        serializers: dict[str, Serializer] | None = None,
        default_format: str = "binary",  # default view format
    ) -> None:
        self.core = AsyncBlobStoreCore(
            adapter,
            container_name,
            cache_mode=cache_mode,
            concurrency_mode=concurrency_mode,
        )
        self.serializers = serializers or {
            "json": JSONSerializer(),
            "binary": BinarySerializer(),
        }

        self._cache = self.core._cache  # for tests
        self.concurrency_mode = self.core.concurrency_mode

        # Create serializer views for all formats
        for fmt in self.serializers:
            view = SerializerView(self, fmt)
            setattr(self, fmt, view)

        # Set default view
        if default_format not in self.serializers:
            raise ValueError(f"Default format '{default_format}' not in serializers")
        self.default_view = getattr(self, default_format)

    async def __aenter__(self) -> "AsyncBlobStore":
        await self.core.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.core.__aexit__(exc_type, exc, tb)

    async def close(self) -> None:
        await self.core.close()

    def _blob_name(self, key: str, serializer: Serializer) -> str:
        return serializer.name_strategy(key)

    # --- Internal format-based methods ---
    async def _get(self, key: str, format: str) -> Any:
        serializer = self.serializers[format]
        blob_name = self._blob_name(key, serializer)
        raw = await self.core.get_bytes(blob_name)
        return serializer.deserialize(raw)

    async def _set(self, key: str, value: Any, format: str) -> None:
        serializer = self.serializers[format]
        raw = serializer.serialize(value)
        blob_name = self._blob_name(key, serializer)
        await self.core.set_bytes(blob_name, raw)

    async def _delete(self, key: str, format: str) -> None:
        serializer = self.serializers[format]
        blob_name = self._blob_name(key, serializer)
        await self.core.delete(blob_name)

    # --- Public default view methods ---
    async def get(self, key: str):
        return await self.default_view.get(key)

    async def set(self, key: str, value: Any):
        return await self.default_view.set(key, value)

    async def delete(self, key: str):
        return await self.default_view.delete(key)

    # --- Pass-through core methods ---
    async def list_keys(self, prefix: str = "") -> list[str]:
        return await self.core.list_keys(prefix)

    async def sync(
        self, etag_behavior: Literal["skip", "overwrite", "raise"] = "skip"
    ) -> None:
        await self.core.sync(etag_behavior)

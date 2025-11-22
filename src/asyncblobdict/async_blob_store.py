import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Literal, cast
from collections import defaultdict

from .storage_protocols import AsyncStorageAdapter

# ---------------------------
# JSON type definitions
# ---------------------------
JSONScalar = str | int | float | bool | None
JSONValue = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
ExtendedJSON = (
    JSONScalar
    | list["ExtendedJSON"]
    | dict[str, "ExtendedJSON"]
    | set["ExtendedJSON"]
    | datetime
)


# ---------------------------
# Enums for configuration
# ---------------------------
class CacheMode(Enum):
    NONE = "none"  # No caching at all
    WRITE_THROUGH = "write_through"  # Cache + write immediately to backend
    WRITE_BACK = "write_back"  # Cache + sync later


class ConcurrencyMode(Enum):
    NONE = "none"  # No concurrency control
    ETAG = "etag"  # Use ETag optimistic concurrency


# ---------------------------
# Exceptions
# ---------------------------
class ConcurrencyError(Exception):
    """Raised when ETag optimistic concurrency check fails."""

    pass


class BlobNotFoundError(Exception):
    """Raised when a requested blob does not exist."""

    pass


# ---------------------------
# JSON serialization helpers
# ---------------------------
def default_encoder(o: Any) -> Any:
    if isinstance(o, set):
        return {"__type__": "set", "items": list(o)}
    if isinstance(o, datetime):
        return {"__type__": "datetime", "value": o.isoformat()}
    raise TypeError(f"Type {type(o)} not serializable to JSON")


def default_decoder(d: dict[str, Any]) -> Any:
    if "__type__" in d:
        if d["__type__"] == "set":
            return set(d["items"])
        if d["__type__"] == "datetime":
            return datetime.fromisoformat(d["value"])
    return d


# ---------------------------
# AsyncBlobStore
# ---------------------------


@dataclass
class CacheEntry:
    value: Any
    etag: str | None


class AsyncBlobStore:
    """
    Backend-agnostic async blob store.

    Handles:
    - JSON/binary serialization
    - Optional caching
    - Optional ETag concurrency control

    Relies on an AsyncStorageAdapter for actual storage operations.
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
        # self._cache: dict[str, tuple[Any, str | None]] = {}
        self._cache: dict[str, CacheEntry] = defaultdict(lambda: CacheEntry(None, None))

    async def __aenter__(self) -> "AsyncBlobStore":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        await self.adapter.close()

    def _blob_name(self, key: str, ext: str) -> str:
        return key if key.endswith(ext) else f"{key}{ext}"

    async def get_json(self, key: str) -> ExtendedJSON:
        """
        Retrieve a JSON blob from storage.

        Args:
            key: The blob key without extension.

        Returns:
            The deserialized JSON object.

        Raises:
            BlobNotFoundError: If the blob does not exist.
            ValueError: If the blob contains invalid JSON.
        """
        if self.cache_mode != CacheMode.NONE and key in self._cache:
            # return self._cache[key][0]
            return self._cache[key].value
        blob_name = self._blob_name(key, ".json")
        blob = self.container.get_blob(blob_name)
        try:
            raw = await blob.download()
        except FileNotFoundError:
            raise BlobNotFoundError(f"Blob '{key}' not found")
        try:
            data = json.loads(raw, object_hook=default_decoder)
            data = cast(ExtendedJSON, data)
        except json.JSONDecodeError:
            raise ValueError(f"Blob '{key}' does not contain valid JSON data")
        etag = (
            await blob.get_etag()
            if self.concurrency_mode == ConcurrencyMode.ETAG
            else None
        )
        if self.cache_mode != CacheMode.NONE:
            # self._cache[key] = (data, etag)
            self._cache[key] = CacheEntry(value=data, etag=etag)
        return data

    async def set_json(
        self, key: str, value: ExtendedJSON, _test_use_stale_etag: bool = False
    ) -> None:
        try:
            json.dumps(value, default=default_encoder)
        except TypeError as e:
            raise ValueError(f"Value for key '{key}' is not JSON-serializable: {e}")
        etag = (
            self._cache[key].etag
            if self.cache_mode != CacheMode.NONE
            else None
        )
        if (
            self.concurrency_mode == ConcurrencyMode.ETAG
            and etag is None
            and not _test_use_stale_etag
        ):
            blob = self.container.get_blob(self._blob_name(key, ".json"))
            try:
                etag = await blob.get_etag()
                print(f"Fetched ETag for blob '{key}': {etag}")
            except FileNotFoundError:
                pass
        if self.cache_mode != CacheMode.NONE:
            self._cache[key] = CacheEntry(value=value, etag=etag)
        if self.cache_mode in (CacheMode.WRITE_THROUGH, CacheMode.NONE):
            await self._upload_json(key, value, etag)

    async def _upload_json(
        self, key: str, value: ExtendedJSON, etag: str | None
    ) -> None:
        blob = self.container.get_blob(self._blob_name(key, ".json"))
        data_bytes = json.dumps(value, default=default_encoder).encode("utf-8")
        try:
            await blob.upload(data_bytes, overwrite=True, if_match=etag)
        except ConcurrencyError:
            raise

    async def get_binary(self, key: str) -> bytes:
        if self.cache_mode != CacheMode.NONE and key in self._cache:
            return self._cache[key].value
        blob = self.container.get_blob(self._blob_name(key, ".bin"))
        try:
            data = await blob.download()
        except FileNotFoundError:
            raise BlobNotFoundError(f"Blob '{key}' not found")
        etag = (
            await blob.get_etag()
            if self.concurrency_mode == ConcurrencyMode.ETAG
            else None
        )
        if self.cache_mode != CacheMode.NONE:
            self._cache[key] = CacheEntry(value=data, etag=etag)
        return data

    async def set_binary(
        self, key: str, data: bytes, _test_use_stale_etag: bool = False
    ) -> None:
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError(f"Binary data for key '{key}' must be bytes or bytearray")
        etag = (
            self._cache[key].etag
            if self.cache_mode != CacheMode.NONE
            else None
        )
        if (
            self.concurrency_mode == ConcurrencyMode.ETAG
            and etag is None
            and not _test_use_stale_etag
        ):
            blob = self.container.get_blob(self._blob_name(key, ".bin"))
            try:
                etag = await blob.get_etag()
            except FileNotFoundError:
                pass
        if self.cache_mode != CacheMode.NONE:
            self._cache[key] = CacheEntry(value=data, etag=etag)
        if self.cache_mode in (CacheMode.WRITE_THROUGH, CacheMode.NONE):
            await self._upload_binary(key, data, etag)

    async def _upload_binary(self, key: str, data: bytes, etag: str | None) -> None:
        blob = self.container.get_blob(self._blob_name(key, ".bin"))
        try:
            await blob.upload(data, overwrite=True, if_match=etag)
        except ConcurrencyError:
            raise

    async def delete(self, key: str) -> None:
        for ext in (".json", ".bin"):
            blob = self.container.get_blob(self._blob_name(key, ext))
            try:
                await blob.delete()
            except FileNotFoundError:
                pass
        if self.cache_mode != CacheMode.NONE and key in self._cache:
            del self._cache[key]

    async def list_keys(self, prefix: str = "") -> list[str]:
        return await self.container.list_blob_names(prefix)

    async def sync_simple(self) -> None:
        if self.cache_mode == CacheMode.NONE:
            raise RuntimeError("Cache is disabled, nothing to sync.")
        for key, cache_entry in self._cache.items():
            if isinstance(cache_entry.value, bytes):
                await self._upload_binary(key, cache_entry.value, cache_entry.etag)
            else:
                await self._upload_json(key, cache_entry.value, cache_entry.etag)

    async def sync(
        self, etag_behavior: Literal["skip", "overwrite", "raise"] = "skip"
    ) -> None:
        if self.cache_mode == CacheMode.NONE:
            raise RuntimeError("Cache is disabled, nothing to sync.")
        # for key, (value, cached_etag) in list(self._cache.items()):
        for key, cache_entry in list(self._cache.items()):
            value = cache_entry.value
            cached_etag = cache_entry.etag
            ext = ".bin" if isinstance(value, (bytes, bytearray)) else ".json"
            blob = self.container.get_blob(self._blob_name(key, ext))
            try:
                latest_etag = await blob.get_etag()
            except FileNotFoundError:
                latest_etag = None
            if (
                self.concurrency_mode == ConcurrencyMode.ETAG
                and cached_etag is not None
                and latest_etag != cached_etag
            ):
                if etag_behavior == "skip":
                    print(
                        f"[Sync] [WARNING] Conflict detected for '{key}', skipping upload."
                    )
                    continue
                elif etag_behavior == "raise":
                    raise ConcurrencyError(
                        f"ETag mismatch for '{key}': cached={cached_etag}, remote={latest_etag}"
                    )
                elif etag_behavior == "overwrite":
                    print(f"[Sync] [INFO] Conflict detected for '{key}', overwriting.")
                    latest_etag = None
            if isinstance(value, (bytes, bytearray)):
                await self._upload_binary(key, value, latest_etag)
            else:
                await self._upload_json(key, value, latest_etag)
            try:
                new_etag = await blob.get_etag()
            except FileNotFoundError:
                new_etag = None
            self._cache[key] = CacheEntry(value=value, etag=new_etag)

        if self.concurrency_mode == ConcurrencyMode.ETAG:
            print(
                f"[Sync] Synced {len(self._cache)} items with ETag protection, behavior={etag_behavior}."
            )
        else:
            print(f"[Sync] Synced {len(self._cache)} items without ETag protection.")

    async def get(self, key: str) -> Any:
        """
        Retrieve a JSON blob from storage.

        Args:
            key: The blob key without extension.

        Returns:
            The deserialized JSON object.

        Raises:
            BlobNotFoundError: If the blob does not exist.
            ValueError: If the blob contains invalid JSON.
        """
        return await self.get_json(key)

    async def set(self, key: str, value: Any) -> None:
        return await self.set_json(key, value)

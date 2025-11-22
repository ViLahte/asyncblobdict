from dataclasses import dataclass
from enum import Enum
from typing import Literal

from .errors import BlobNotFoundError, ConcurrencyError
from .serializers import BinarySerializer, ExtendedJSON, JSONSerializer
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


class AsyncCoreBlobStore:
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
        # Cache is now keyed by actual blob_name
        self._cache: dict[str, CacheEntry] = {}

    async def __aenter__(self) -> "AsyncCoreBlobStore":
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
        """
        Retrieve raw bytes for a blob.
        Uses cache if enabled and blob_name is present.
        """
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
        force_no_etag_check: bool = False,
    ) -> None:
        """
        Store raw bytes for a blob.
        In WRITE_BACK mode, only updates cache.
        In WRITE_THROUGH or NONE mode, writes immediately to backend.
        """
        if self.cache_mode != CacheMode.NONE:
            self._cache[blob_name] = CacheEntry(value=data, etag=etag)

        if self.cache_mode in (CacheMode.WRITE_THROUGH, CacheMode.NONE):
            if (
                self.concurrency_mode == ConcurrencyMode.ETAG
                and not force_no_etag_check
            ):
                if etag is None:
                    blob = self.container.get_blob(blob_name)
                    etag = await self.get_etag_or_none(blob)

            blob = self.container.get_blob(blob_name)
            await blob.upload(data, overwrite=True, if_match=etag)

    async def delete(self, blob_name: str) -> None:
        """
        Delete blob from backend and remove from cache if present.
        """
        blob = self.container.get_blob(blob_name)
        try:
            await blob.delete()
        except BlobNotFoundError:
            pass

        if blob_name in self._cache:
            del self._cache[blob_name]

    async def list_keys(self, prefix: str = "") -> list[str]:
        """
        List blob names in container.
        """
        return await self.container.list_blob_names(prefix)

    async def sync(
        self, etag_behavior: Literal["skip", "overwrite", "raise"] = "skip"
    ) -> None:
        """
        Flush cached blobs to backend.
        """
        if self.cache_mode == CacheMode.NONE:
            # TODO Should probably clean cache if mode is changed mid way.
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


class AsyncFormatBlobStore:
    """
    Format-aware store: wraps AsyncCoreBlobStore to add JSON/binary serialization.
    Responsible for naming (.json) and format validation.
    """

    def __init__(
        self,
        core_store: AsyncCoreBlobStore,
        json_serializer: JSONSerializer | None = None,
        binary_serializer: BinarySerializer | None = None,
    ) -> None:
        self.core = core_store
        self.json_serializer = json_serializer or JSONSerializer()
        self.binary_serializer = binary_serializer or BinarySerializer()

    def _blob_name_json(self, key: str) -> str:
        assert isinstance(key, str)
        return f"{key}.json"

    def _blob_name_binary(self, key: str) -> str:
        assert isinstance(key, str)
        return key

    async def get_json(self, key: str) -> ExtendedJSON:
        blob_name = self._blob_name_json(key)
        raw = await self.core.get_bytes(blob_name)  # cache key now always blob_name
        try:
            return self.json_serializer.deserialize(raw)
        except Exception as e:
            raise ValueError(f"Blob '{key}' is not valid JSON") from e

    async def set_json(
        self, key: str, value: ExtendedJSON, force_no_etag_check: bool = False
    ) -> None:
        raw = self.json_serializer.serialize(value)
        blob_name = self._blob_name_json(key)
        await self.core.set_bytes(
            blob_name,
            raw,
            force_no_etag_check=force_no_etag_check,
        )

    async def get_binary(self, key: str) -> bytes:
        blob_name = self._blob_name_binary(key)
        return await self.core.get_bytes(blob_name)

    async def set_binary(
        self, key: str, data: bytes, force_no_etag_check: bool = False
    ) -> None:
        raw = self.binary_serializer.serialize(data)
        blob_name = self._blob_name_binary(key)
        await self.core.set_bytes(
            blob_name,
            raw,
            force_no_etag_check=force_no_etag_check,
        )

    async def delete_json(self, key: str) -> None:
        blob_name = self._blob_name_json(key)
        await self.core.delete(blob_name)

    async def delete_binary(self, key: str) -> None:
        blob_name = self._blob_name_binary(key)
        await self.core.delete(blob_name)

    async def list_keys(self, prefix: str = "") -> list[str]:
        return await self.core.list_keys(prefix)


class AsyncBlobStore:
    """
    Compatibility wrapper for old AsyncBlobStore API.
    """

    def __init__(
        self,
        adapter: AsyncStorageAdapter,
        container_name: str,
        cache_mode: CacheMode = CacheMode.NONE,
        concurrency_mode: ConcurrencyMode = ConcurrencyMode.NONE,
        json_serializer: JSONSerializer | None = None,
        binary_serializer: BinarySerializer | None = None,
    ) -> None:
        self._core = AsyncCoreBlobStore(
            adapter,
            container_name,
            cache_mode=cache_mode,
            concurrency_mode=concurrency_mode,
        )
        self._format = AsyncFormatBlobStore(
            self._core,
            json_serializer=json_serializer,
            binary_serializer=binary_serializer,
        )
        # Compatibility: expose _cache for tests
        self._cache = self._core._cache
        self.concurrency_mode = self._core.concurrency_mode

    async def __aenter__(self) -> "AsyncBlobStore":
        await self._core.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self._core.__aexit__(exc_type, exc, tb)

    async def close(self) -> None:
        await self._core.close()

    async def get_json(self, key: str) -> ExtendedJSON:
        return await self._format.get_json(key)

    async def set_json(self, key: str, value: ExtendedJSON) -> None:
        await self._format.set_json(key, value)

    async def get_binary(self, key: str) -> bytes:
        return await self._format.get_binary(key)

    async def set_binary(self, key: str, data: bytes) -> None:
        await self._format.set_binary(key, data)

    async def delete_json(self, key: str) -> None:
        await self._format.delete_json(key)

    async def delete_binary(self, key: str) -> None:
        await self._format.delete_binary(key)

    async def list_keys(self, prefix: str = "") -> list[str]:
        return await self._format.list_keys(prefix)

    async def sync(
        self, etag_behavior: Literal["skip", "overwrite", "raise"] = "skip"
    ) -> None:
        await self._core.sync(etag_behavior)

    async def get(self, key: str) -> ExtendedJSON:
        return await self.get_json(key)

    async def set(self, key: str, value: ExtendedJSON) -> None:
        return await self.set_json(key, value)

    async def delete(self, key: str) -> None:
        await self.delete_json(key)

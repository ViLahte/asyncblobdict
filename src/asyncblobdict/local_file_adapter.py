import asyncio
import hashlib
from pathlib import Path
from .async_blob_store import ConcurrencyError
from .storage_protocols import (
    AsyncBlobHandle,
    AsyncContainerHandle,
    AsyncStorageAdapter,
)


def _ensure_within(base: Path, target: Path, strict: bool = True) -> Path:
    """
    Resolve target path and ensure it is inside base path.
    strict=True will fail if the target does not exist (good for read/delete).
    strict=False allows non-existing targets (good for upload), but still checks parent dir strictly.
    """
    base_resolved = base.resolve(strict=True)
    if strict:
        target_resolved = target.resolve(strict=True)
    else:
        # Resolve parent strictly to catch symlink escapes
        target.parent.resolve(strict=True)
        target_resolved = target.resolve()
    if not str(target_resolved).startswith(str(base_resolved)):
        raise ValueError(
            f"Path {target_resolved} escapes base directory {base_resolved}"
        )
    return target_resolved


class LocalFileAdapter(AsyncStorageAdapter):
    """Local filesystem adapter for AsyncBlobStore."""

    def __init__(self, base_path: str):
        self._base_path = Path(base_path).resolve()
        self._base_path.mkdir(parents=True, exist_ok=True)

    def get_container(self, container_name: str) -> AsyncContainerHandle:
        container_path = _ensure_within(
            self._base_path, self._base_path / container_name, strict=False
        )
        container_path.mkdir(parents=True, exist_ok=True)
        return _LocalContainerHandle(container_path)

    async def close(self) -> None:
        pass


class _LocalContainerHandle(AsyncContainerHandle):
    def __init__(self, container_path: Path):
        self._container_path = container_path

    def get_blob(self, blob_name: str) -> AsyncBlobHandle:
        blob_path = _ensure_within(
            self._container_path, self._container_path / blob_name, strict=False
        )
        return _LocalBlobHandle(blob_path, self._container_path)

    async def list_blob_names(self, prefix: str = "") -> list[str]:
        files: list[str] = []
        for path in self._container_path.rglob("*"):
            if path.is_file():
                # Strict resolve to catch symlink escapes
                _ensure_within(self._container_path, path, strict=True)
                rel_path = path.relative_to(self._container_path).as_posix()
                if rel_path.startswith(prefix):
                    files.append(rel_path)
        return files


# Global lock registry for concurrency safety
_lock_registry: dict[str, asyncio.Lock] = {}


def _get_global_lock(path: Path) -> asyncio.Lock:
    key = str(path.resolve())
    if key not in _lock_registry:
        _lock_registry[key] = asyncio.Lock()
    return _lock_registry[key]


class _LocalBlobHandle(AsyncBlobHandle):
    def __init__(self, file_path: Path, container_path: Path):
        self._file_path = file_path
        self._container_path = container_path
        self._lock = _get_global_lock(file_path)

    async def download(self) -> bytes:
        _ensure_within(self._container_path, self._file_path, strict=True)
        if not self._file_path.exists():
            raise FileNotFoundError()
        async with self._lock:
            return self._file_path.read_bytes()

    async def upload(
        self, data: bytes, overwrite: bool = True, if_match: str | None = None
    ) -> None:
        _ensure_within(self._container_path, self._file_path, strict=False)
        if self._file_path.exists() and not overwrite:
            raise FileExistsError(f"Blob {self._file_path} already exists")
        if if_match is not None:
            current_etag = await self.get_etag()
            if current_etag != if_match:
                raise ConcurrencyError(f"ETag mismatch for blob '{self._file_path}'")
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        async with self._lock:
            self._file_path.write_bytes(data)

    async def delete(self) -> None:
        _ensure_within(self._container_path, self._file_path, strict=True)
        if not self._file_path.exists():
            raise FileNotFoundError()
        self._file_path.unlink()

    async def get_etag(self) -> str | None:
        _ensure_within(self._container_path, self._file_path, strict=True)
        if not self._file_path.exists():
            raise FileNotFoundError()
        # Use mtime+size cache to avoid recomputing MD5 unnecessarily
        stat = self._file_path.stat()
        cache_key = (self._file_path.resolve(), stat.st_mtime, stat.st_size)
        if not hasattr(self, "_etag_cache"):
            self._etag_cache = {}
        if cache_key in self._etag_cache:
            return self._etag_cache[cache_key]
        content = self._file_path.read_bytes()
        etag = hashlib.md5(content).hexdigest()
        self._etag_cache[cache_key] = etag
        return etag



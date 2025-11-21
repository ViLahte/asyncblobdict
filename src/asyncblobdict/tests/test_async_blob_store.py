import asyncio
import hashlib
import os
import pickle
import random
import sys
import uuid
from datetime import datetime

import pytest
from dotenv import load_dotenv

from asyncblobdict import (
    AsyncBlobStore,
    AzureBlobAdapter,
    BlobNotFoundError,
    CacheMode,
    ConcurrencyError,
    ConcurrencyMode,
    LocalFileAdapter,
)
from asyncblobdict.local_file_adapter import _LocalBlobHandle, _LocalContainerHandle

load_dotenv()

# Azure config
CONN_STR = os.environ.get("AZURE_CONN_STR")
CONTAINER_NAME = os.environ.get("AZURE_CONTAINER")

# Local config
LOCAL_CONTAINER = "test_container"


def unique_key(suffix: str) -> str:
    return f"test_{suffix}_{uuid.uuid4()}"


# ---------------------------
# Backend factory
# ---------------------------
def azure_backend():
    if not CONN_STR or not CONTAINER_NAME:
        print("[TEST] Azure backend not configured — skipping Azure tests.")
        pytest.skip(
            "Azure backend not configured (AZURE_CONN_STR / AZURE_CONTAINER missing)"
        )
    return AzureBlobAdapter.from_connection_string(CONN_STR), CONTAINER_NAME


def local_backend():
    return LocalFileAdapter(LOCAL_CONTAINER), LOCAL_CONTAINER


# ---------------------------
# Parametrize backends
# ---------------------------
@pytest.fixture(
    params=[
        pytest.param("azure", marks=pytest.mark.azure),
        pytest.param("local", marks=pytest.mark.local),
    ]
)
def backend(request, tmp_path):
    """Fixture that provides either Azure or local backend."""
    if request.param == "azure":
        if not CONN_STR or not CONTAINER_NAME:
            pytest.skip(
                "Azure backend not configured (AZURE_CONN_STR / AZURE_CONTAINER missing)"
            )

        adapter, container = azure_backend()

        # Cleanup for Azure before test
        from azure.storage.blob import BlobServiceClient

        blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
        container_client = blob_service_client.get_container_client(container)
        for blob in container_client.list_blobs(name_starts_with="test_"):
            container_client.delete_blob(blob.name)

        yield adapter, container

        # Cleanup for Azure after test
        for blob in container_client.list_blobs(name_starts_with="test_"):
            container_client.delete_blob(blob.name)

    elif request.param == "local":
        # Use pytest's tmp_path for safe temporary storage
        adapter = LocalFileAdapter(str(tmp_path))
        container = LOCAL_CONTAINER
        yield adapter, container
        # No manual deletion needed — tmp_path is auto-cleaned by pytest


# ---------------------------
# Tests
# ---------------------------


@pytest.mark.asyncio
async def test_json_set_get(backend):
    adapter, container = backend
    key = unique_key("user_settings")
    async with (
        AsyncBlobStore(
            adapter,
            container,
            cache_mode=CacheMode.WRITE_THROUGH,
            concurrency_mode=ConcurrencyMode.ETAG,
        ) as store_a,
        AsyncBlobStore(
            adapter,
            container,
            cache_mode=CacheMode.NONE,
            concurrency_mode=ConcurrencyMode.ETAG,
        ) as store_b,
    ):
        await store_a.set(
            key,
            {"theme": "dark", "tags": {"ml", "ai"}, "last_login": datetime.utcnow()},
        )
        val_a = await store_a.get(key)
        val_b = await store_b.get(key)
        assert val_a == val_b


@pytest.mark.asyncio
async def test_binary_set_get(backend):
    adapter, container = backend
    key = unique_key("models_model1")
    async with (
        AsyncBlobStore(
            adapter,
            container,
            cache_mode=CacheMode.WRITE_THROUGH,
            concurrency_mode=ConcurrencyMode.ETAG,
        ) as store_a,
        AsyncBlobStore(
            adapter,
            container,
            cache_mode=CacheMode.NONE,
            concurrency_mode=ConcurrencyMode.ETAG,
        ) as store_b,
    ):
        data_bytes = pickle.dumps({"a": 1, "b": 2})
        await store_a.set_binary(key, data_bytes)
        loaded_bytes = await store_b.get_binary(key)
        assert pickle.loads(loaded_bytes) == {"a": 1, "b": 2}


@pytest.mark.asyncio
async def test_missing_blob(backend):
    adapter, container = backend
    key = unique_key("nonexistent_key")
    async with AsyncBlobStore(
        adapter,
        container,
        cache_mode=CacheMode.NONE,
        concurrency_mode=ConcurrencyMode.ETAG,
    ) as store_b:
        with pytest.raises(BlobNotFoundError):
            await store_b.get(key)


@pytest.mark.asyncio
async def test_non_json_serializable(backend):
    adapter, container = backend
    key = unique_key("bad_data")
    async with AsyncBlobStore(
        adapter,
        container,
        cache_mode=CacheMode.WRITE_THROUGH,
        concurrency_mode=ConcurrencyMode.ETAG,
    ) as store_a:
        with pytest.raises(ValueError):
            await store_a.set(key, lambda x: x)


@pytest.mark.asyncio
async def test_etag_conflict(backend):
    adapter, container = backend
    key = unique_key("user_settings_conflict")
    async with (
        AsyncBlobStore(
            adapter,
            container,
            cache_mode=CacheMode.WRITE_THROUGH,
            concurrency_mode=ConcurrencyMode.ETAG,
        ) as store_a,
        AsyncBlobStore(
            adapter,
            container,
            cache_mode=CacheMode.NONE,
            concurrency_mode=ConcurrencyMode.ETAG,
        ) as store_b,
    ):
        # Step 1: Write initial value
        await store_a.set(key, {"theme": "dark"})

        # Step 2: Force read to get real ETag
        store_a._cache.clear()
        await store_a.get(key)
        old_etag = store_a._cache[key].etag
        assert old_etag is not None, "ETag should not be None for conflict test"

        # Step 3: Overwrite with store_b (disable ETag check)
        store_b.concurrency_mode = ConcurrencyMode.NONE
        await store_b.set(key, {"theme": "light"})

        # Step 4: Try to write with stale ETag
        with pytest.raises(ConcurrencyError):
            await store_a.set_json(
                key,
                {"theme": "dark again", "random": random.randint(0, 1000)},
                _test_use_stale_etag=True,
            )


@pytest.mark.asyncio
async def test_delete(backend):
    adapter, container = backend
    key = unique_key("user_settings_delete")
    async with (
        AsyncBlobStore(
            adapter,
            container,
            cache_mode=CacheMode.WRITE_THROUGH,
            concurrency_mode=ConcurrencyMode.ETAG,
        ) as store_a,
        AsyncBlobStore(
            adapter,
            container,
            cache_mode=CacheMode.NONE,
            concurrency_mode=ConcurrencyMode.ETAG,
        ) as store_b,
    ):
        await store_a.set(key, {"theme": "dark"})
        await store_a.delete(key)
        with pytest.raises(BlobNotFoundError):
            await store_b.get(key)


@pytest.mark.asyncio
async def test_list_keys(backend):
    adapter, container = backend
    key1 = unique_key("misc_data1")
    key2 = unique_key("misc_data2")
    async with (
        AsyncBlobStore(
            adapter,
            container,
            cache_mode=CacheMode.WRITE_THROUGH,
            concurrency_mode=ConcurrencyMode.ETAG,
        ) as store_a,
        AsyncBlobStore(
            adapter,
            container,
            cache_mode=CacheMode.NONE,
            concurrency_mode=ConcurrencyMode.ETAG,
        ) as store_b,
    ):
        await store_a.set(key1, {"x": 1})
        await store_a.set_binary(key2, b"hello")
        keys = await store_b.list_keys("test_")
        assert any(key1 + ".json" == k for k in keys)
        assert any(key2 + ".bin" == k for k in keys)


@pytest.mark.asyncio
async def test_sync(backend):
    adapter, container = backend
    key = unique_key("sync_test")
    async with (
        AsyncBlobStore(
            adapter,
            container,
            cache_mode=CacheMode.WRITE_BACK,
            concurrency_mode=ConcurrencyMode.ETAG,
        ) as store_c,
        AsyncBlobStore(
            adapter,
            container,
            cache_mode=CacheMode.NONE,
            concurrency_mode=ConcurrencyMode.ETAG,
        ) as store_b,
    ):
        await store_c.set(key, {"y": 42})
        with pytest.raises(BlobNotFoundError):
            await store_b.get(key)
        await store_c.sync()
        val_b = await store_b.get(key)
        assert val_b == {"y": 42}


@pytest.mark.asyncio
async def test_list_blobs_with_prefix(backend):
    adapter, container = backend
    ch = adapter.get_container(container)

    await ch.get_blob("a1.txt").upload(b"x")
    await ch.get_blob("a2.txt").upload(b"x")
    await ch.get_blob("b1.txt").upload(b"x")

    names = await ch.list_blob_names(prefix="a")
    assert set(names) >= {"a1.txt", "a2.txt"}
    assert "b1.txt" not in names


@pytest.mark.asyncio
async def test_upload_with_overwrite_false(backend):
    adapter, container = backend
    ch = adapter.get_container(container)
    blob = ch.get_blob("exists.txt")
    await blob.upload(b"data")
    with pytest.raises(FileExistsError):
        await blob.upload(b"newdata", overwrite=False)


@pytest.mark.asyncio
async def test_etag_retrieval(backend):
    adapter, container = backend
    ch = adapter.get_container(container)
    blob = ch.get_blob("etag.txt")
    await blob.upload(b"etagtest")
    etag = await blob.get_etag()
    assert isinstance(etag, str)
    assert etag != ""


@pytest.mark.asyncio
async def test_concurrency_error_on_etag_mismatch(backend):
    adapter, container = backend
    ch = adapter.get_container(container)
    blob = ch.get_blob("concurrent.txt")
    await blob.upload(b"first")
    etag = await blob.get_etag()
    with pytest.raises(ConcurrencyError):
        await blob.upload(b"second", if_match="wrong-etag")


@pytest.mark.asyncio
@pytest.mark.local
async def test_global_concurrency_lock(backend):
    adapter, container = backend
    if not isinstance(adapter, LocalFileAdapter):
        pytest.skip("Global lock test only applies to LocalFileAdapter")

    container_handle = adapter.get_container(container)
    blob_handle1 = container_handle.get_blob("shared.txt")
    blob_handle2 = container_handle.get_blob("shared.txt")

    async def writer(handle, data):
        await handle.upload(data.encode())

    # Run two uploads concurrently
    await asyncio.gather(writer(blob_handle1, "first"), writer(blob_handle2, "second"))

    # Only one of the writes should be present (last one wins, but no corruption)
    content = (await blob_handle1.download()).decode()
    assert content in ("first", "second")
    assert content != "firstsecond"  # No interleaving corruption


@pytest.mark.asyncio
@pytest.mark.local
async def test_etag_caching(backend, monkeypatch):
    adapter, container = backend
    if not isinstance(adapter, LocalFileAdapter):
        pytest.skip("ETag caching test only applies to LocalFileAdapter")

    container_handle = adapter.get_container(container)
    blob_handle = container_handle.get_blob("etag_test.txt")

    await blob_handle.upload(b"hello world")

    call_count = {"md5": 0}

    original_md5 = hashlib.md5

    def counting_md5(*args, **kwargs):
        call_count["md5"] += 1
        return original_md5(*args, **kwargs)

    monkeypatch.setattr(hashlib, "md5", counting_md5)

    # First call should compute MD5
    etag1 = await blob_handle.get_etag()
    assert call_count["md5"] == 1

    # Second call should use cache (no new MD5 computation)
    etag2 = await blob_handle.get_etag()
    assert call_count["md5"] == 1
    assert etag1 == etag2

    # Modify file to invalidate cache
    await blob_handle.upload(b"changed")
    etag3 = await blob_handle.get_etag()
    assert call_count["md5"] == 2
    assert etag3 != etag1


@pytest.mark.asyncio
@pytest.mark.local
async def test_local_path_traversal_protection(backend):
    adapter, container = backend
    if not isinstance(adapter, LocalFileAdapter):
        pytest.skip("Path traversal protection test only applies to LocalFileAdapter")

    malicious_blob_name = "../../etc/passwd"
    container_handle = adapter.get_container(container)

    # Accept either ValueError or FileNotFoundError for malicious paths
    with pytest.raises((ValueError, FileNotFoundError)) as excinfo:
        container_handle.get_blob(malicious_blob_name)

    # If it's a ValueError, check message
    if isinstance(excinfo.value, ValueError):
        assert "escapes base directory" in str(excinfo.value)

    # Also test malicious container name
    with pytest.raises((ValueError, FileNotFoundError)):
        adapter.get_container("../outside_container")


@pytest.mark.asyncio
@pytest.mark.local
async def test_local_delete_outside_protection(backend, tmp_path):
    adapter, container = backend
    if not isinstance(adapter, LocalFileAdapter):
        pytest.skip("Delete safety test only applies to LocalFileAdapter")

    container_handle = adapter.get_container(container)

    # Create a file outside the container
    outside_file = tmp_path.parent / "outside.txt"
    outside_file.write_text("secret")

    # Try to make a blob handle pointing to it (bypassing normal get_blob)
    assert isinstance(container_handle, _LocalContainerHandle)
    malicious_blob = _LocalBlobHandle(outside_file, container_handle._container_path)

    with pytest.raises(ValueError):
        await malicious_blob.delete()

    assert outside_file.exists(), "Outside file should not be deleted"


@pytest.mark.asyncio
@pytest.mark.local
async def test_symlink_outside_protection(backend, tmp_path):
    if not hasattr(os, "symlink"):
        pytest.skip("Symlinks not supported on this platform")
    if sys.platform == "win32":
        # Windows requires admin or Developer Mode for symlinks
        try:
            test_link = tmp_path / "test_link"
            test_target = tmp_path / "test_target"
            test_target.write_text("x")
            test_link.symlink_to(test_target)
        except OSError:
            pytest.skip("Symlink creation not permitted on this Windows system")
    adapter, container = backend
    if not isinstance(adapter, LocalFileAdapter):
        pytest.skip("Symlink protection test only applies to LocalFileAdapter")

    container_handle = adapter.get_container(container)
    assert isinstance(container_handle, _LocalContainerHandle)

    # Create a file outside the container
    outside_file = tmp_path.parent / "outside.txt"
    outside_file.write_text("secret")

    # Create a symlink inside the container pointing to the outside file
    symlink_path = container_handle._container_path / "link.txt"
    symlink_path.symlink_to(outside_file)

    blob_handle = container_handle.get_blob("link.txt")

    # Download should fail due to symlink escape
    with pytest.raises(ValueError):
        await blob_handle.download()

    # Delete should also fail
    with pytest.raises(ValueError):
        await blob_handle.delete()

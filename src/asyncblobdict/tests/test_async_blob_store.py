import os
import uuid
import pickle
import random
from datetime import datetime

import pytest
from dotenv import load_dotenv

from asyncblobdict import (
    AsyncBlobStore,
    ConcurrencyError,
    BlobNotFoundError,
    CacheMode,
    ConcurrencyMode,
    AzureBlobAdapter,
    LocalFileAdapter,
)

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

from typing import Any

from .async_blob_store import ConcurrencyError
from azure.core.exceptions import ResourceModifiedError, ResourceNotFoundError
from azure.storage.blob.aio import BlobServiceClient

from .storage_protocols import (
    AsyncBlobHandle,
    AsyncContainerHandle,
    AsyncStorageAdapter,
)


class AzureBlobAdapter(AsyncStorageAdapter):
    """Azure Blob Storage adapter for AsyncBlobStore."""

    def __init__(self, blob_service_client: BlobServiceClient):
        """
        Create an adapter from an existing BlobServiceClient.
        This allows custom authentication and configuration.
        """
        self._client = blob_service_client

    @classmethod
    def from_connection_string(cls, connection_string: str) -> "AzureBlobAdapter":
        """
        Convenience builder: create adapter from a connection string.
        """
        client = BlobServiceClient.from_connection_string(connection_string)
        return cls(client)

    def get_container(self, container_name: str) -> AsyncContainerHandle:
        return _AzureContainerHandle(self._client.get_container_client(container_name))

    async def close(self) -> None:
        await self._client.close()


class _AzureContainerHandle(AsyncContainerHandle):
    def __init__(self, container_client):
        self._container_client = container_client

    def get_blob(self, blob_name: str) -> AsyncBlobHandle:
        return _AzureBlobHandle(self._container_client.get_blob_client(blob_name))

    async def list_blob_names(self, prefix: str = "") -> list[str]:
        names: list[str] = []
        async for blob in self._container_client.list_blobs(name_starts_with=prefix):
            names.append(blob.name)
        return names


class _AzureBlobHandle(AsyncBlobHandle):
    def __init__(self, blob_client):
        self._blob_client = blob_client

    async def download(self) -> bytes:
        try:
            stream = await self._blob_client.download_blob()
            return await stream.readall()
        except ResourceNotFoundError:
            raise FileNotFoundError()

    async def upload(
        self, data: bytes, overwrite: bool = True, if_match: str | None = None
    ) -> None:
        kwargs: dict[str, Any] = {"overwrite": overwrite}
        if if_match:
            kwargs["if_match"] = if_match
        try:
            await self._blob_client.upload_blob(data, **kwargs)
        except ResourceModifiedError:
            raise ConcurrencyError(
                f"ETag mismatch for blob '{self._blob_client.blob_name}'"
            )

    async def delete(self) -> None:
        try:
            await self._blob_client.delete_blob()
        except ResourceNotFoundError:
            raise FileNotFoundError()

    async def get_etag(self) -> str | None:
        try:
            props = await self._blob_client.get_blob_properties()
            return props.etag
        except ResourceNotFoundError:
            raise FileNotFoundError()

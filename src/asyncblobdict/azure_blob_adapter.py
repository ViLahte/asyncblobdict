import mimetypes
from typing import Any

from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)
from azure.storage.blob.aio import BlobServiceClient

from .errors import BlobNotFoundError, ConcurrencyError
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
            raise BlobNotFoundError(f"Blob '{self._blob_client.blob_name}' not found")

    async def upload(
        self,
        data: bytes,
        overwrite: bool = True,
        if_match: str | None = None,
        content_type: str | None = None,
    ) -> None:
        """Note: Guesses content type if not provided."""

        if content_type is None:
            guessed, _ = mimetypes.guess_type(self._blob_client.blob_name)
            content_type = guessed or "application/octet-stream"

        kwargs: dict[str, Any] = {"overwrite": overwrite, "content_type": content_type}
        if if_match:
            kwargs["if_match"] = if_match
        try:
            await self._blob_client.upload_blob(data, **kwargs)
        except ResourceModifiedError:
            raise ConcurrencyError(
                f"ETag mismatch for blob '{self._blob_client.blob_name}'"
            )
        except ResourceExistsError:
            raise FileExistsError(
                f"Blob '{self._blob_client.blob_name}' already exists"
            )
        except HttpResponseError as e:
            if getattr(e, "status_code", None) == 412:
                raise ConcurrencyError(
                    f"ETag mismatch for blob '{self._blob_client.blob_name}'"
                )
            raise

    async def delete(self) -> None:
        try:
            await self._blob_client.delete_blob()
        except ResourceNotFoundError:
            raise BlobNotFoundError(f"Blob '{self._blob_client.blob_name}' not found")

    async def get_etag(self) -> str | None:
        try:
            props = await self._blob_client.get_blob_properties()
            return props.etag
        except ResourceNotFoundError:
            raise BlobNotFoundError(f"Blob '{self._blob_client.blob_name}' not found")

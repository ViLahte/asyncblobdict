![Tests](https://github.com/ViLahte/asyncblobdict/actions/workflows/tests.yml/badge.svg)
[![codecov](https://codecov.io/gh/ViLahte/asyncblobdict/branch/main/graph/badge.svg)](https://codecov.io/gh/ViLahte/asyncblobdict)


# asyncblobdict


**asyncblobdict** is a minimal async‑friendly key–value store for JSON and binary data, backed by either:

- Local filesystem
- Azure Blob Storage

It lets you store and retrieve Python data by key, with optional caching and optimistic concurrency via ETags.
Swap backends without changing your application code.

---

## Features

- **Backends**: Local filesystem or Azure Blob Storage
- **Data types**:
  - JSON‑serializable (`dict`, `list`, `set`, `datetime`, etc.)
  - Arbitrary binary (`bytes`, `bytearray`)
- **Caching modes**:
  - `NONE` - always read/write directly to backend
  - `WRITE_THROUGH` - cache and immediately write to backend
  - `WRITE_BACK` - cache and sync later
- **Concurrency modes**:
  - `NONE` - no concurrency control
  - `ETAG` - optimistic concurrency using ETags

---

## Installation

From PyPI:

```bash
pip install asyncblobdict
```

For development (editable install):

```bash
git clone https://github.com/ViLahte/asyncblobdict.git
cd asyncblobdict
pip install -e .
```

---

## Quick Start

### Local backend example

```python
import asyncio
import pickle
from datetime import datetime
from asyncblobdict import AsyncBlobStore, CacheMode, ConcurrencyMode, LocalFileAdapter, BlobNotFoundError

async def main():
    adapter = LocalFileAdapter("./local_blob_storage")
    async with AsyncBlobStore(
        adapter,
        "demo_container",
        cache_mode=CacheMode.WRITE_THROUGH,
        concurrency_mode=ConcurrencyMode.ETAG,
    ) as store:
        # Store JSON
        config = {"learning_rate": 0.01, "layers": [64, 128, 256], "created_at": datetime.utcnow()}
        await store.set_json("demo/config", config)

        # Retrieve JSON
        loaded_config = await store.get_json("demo/config")
        print("Loaded config:", loaded_config)

        # Store binary
        model_bytes = pickle.dumps({"weights": [0.1, 0.2, 0.3], "bias": 0.5})
        await store.set_binary("demo/model.bin", model_bytes)

        # Retrieve binary
        loaded_model = pickle.loads(await store.get_binary("demo/model.bin"))
        print("Loaded model:", loaded_model)

        # List keys
        print("Keys:", await store.list_keys())

        # Delete a key
        await store.delete("demo/config")
        try:
            await store.get_json("demo/config")
        except BlobNotFoundError:
            print("Config deleted successfully.")

if __name__ == "__main__":
    asyncio.run(main())
```

---

### Azure backend example

Requires environment variables:

```env
AZURE_CONN_STR=your-azure-connection-string
AZURE_CONTAINER=your-container-name
```

```python
import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from asyncblobdict import AsyncBlobStore, CacheMode, ConcurrencyMode, AzureBlobAdapter

load_dotenv()

async def main():
    adapter = AzureBlobAdapter.from_connection_string(os.environ["AZURE_CONN_STR"])
    async with AsyncBlobStore(
        adapter,
        os.environ["AZURE_CONTAINER"],
        cache_mode=CacheMode.WRITE_THROUGH,
        concurrency_mode=ConcurrencyMode.ETAG,
    ) as store:
        await store.set_json("demo/config", {"created_at": datetime.utcnow()})
        print(await store.get_json("demo/config"))

if __name__ == "__main__":
    asyncio.run(main())
```

---

## Syncing in `WRITE_BACK` mode

When using `CacheMode.WRITE_BACK`, changes are cached locally until you call:

```python
await store.sync(etag_behavior="skip")  # options: "skip", "overwrite", "raise"
```

---

## Testing

We use `pytest`:

```bash
pytest
```

Azure tests are skipped automatically if environment variables are not set.
You can also run only local tests:

```bash
pytest -m "not azure"
```

Or only Azure tests:

```bash
pytest -m azure
```

For code coverage html report:

```bash
pytest --cov=src --cov-report=html
```

---

## License

MIT License

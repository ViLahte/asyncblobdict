# asyncblobdict

![Tests](https://github.com/ViLahte/asyncblobdict/actions/workflows/tests.yml/badge.svg)
[![codecov](https://codecov.io/gh/ViLahte/asyncblobdict/branch/main/graph/badge.svg)](https://codecov.io/gh/ViLahte/asyncblobdict)

**asyncblobdict** is a minimal async‑friendly key–value store for JSON and binary data, backed by either:

- Local filesystem
- Azure Blob Storage

It lets you store and retrieve Python data by key, with optional caching and optimistic concurrency via ETags, and swap backends without changing your application code.

---

## Features

- **Backends**: Local filesystem or Azure Blob Storage
- **Data types**:
  - JSON‑serializable (`dict`, `list`, `set`, `datetime`, etc.)
  - Arbitrary binary (`bytes`, `bytearray`)
- **Caching modes**:
  - `NONE` — always read/write directly to backend
  - `WRITE_THROUGH` — cache and immediately write to backend
  - `WRITE_BACK` — cache and sync later
- **Concurrency modes**:
  - `NONE` — no concurrency control
  - `ETAG` — optimistic concurrency using ETags
- **Serializer views**:
  - `store.json` — JSON serializer view
  - `store.binary` — binary serializer view
  - `store.default_view` — default serializer view (format chosen at init)

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
from asyncblobdict import AsyncBlobStore, CacheMode, ConcurrencyMode, LocalFileAdapter

async def main():
    adapter = LocalFileAdapter("./local_blob_storage")
    async with AsyncBlobStore(
        adapter,
        "demo_container",
        cache_mode=CacheMode.WRITE_THROUGH,
        concurrency_mode=ConcurrencyMode.ETAG,
        default_format="json",  # default view is JSON
    ) as store:
        # Store JSON (default view)
        await store.set("demo/config", {"learning_rate": 0.01, "created_at": datetime.utcnow()})

        # Retrieve JSON
        print(await store.get("demo/config"))

        # Store binary explicitly
        model_bytes = pickle.dumps({"weights": [0.1, 0.2, 0.3], "bias": 0.5})
        await store.binary.set("demo/model.bin", model_bytes)

        # Retrieve binary
        print(await store.binary.get("demo/model.bin"))

        # List keys
        print(await store.list_keys())

        # Delete a key
        await store.delete("demo/config")

if __name__ == "__main__":
    asyncio.run(main())
```

---

## API Reference (Simplified)

All methods are **async**.

### Default view (format chosen at init)
```python
await store.set(key: str, value: Any)
await store.get(key: str) -> Any
await store.delete(key: str)
```

### Format-specific views
```python
await store.json.set(key, value)     # JSON serializer
await store.json.get(key)            # returns Python object
await store.binary.set(key, bytes)   # Binary serializer
await store.binary.get(key)          # returns bytes
```

### Other methods
```python
await store.list_keys(prefix: str = "") -> list[str]
await store.sync(etag_behavior="skip")  # flush WRITE_BACK cache
```

---

## Adding Your Own Serializer

You can add new formats (e.g., YAML) by passing a `serializers` dict to `AsyncBlobStore`.

```python
import yaml

from asyncblobdict import (
    AsyncBlobStore,
    BinarySerializer,
    JSONSerializer,
    LocalFileAdapter,
    Serializer,
)


class YAMLSerializer(Serializer):
    def serialize(self, value):
        return yaml.dump(value).encode("utf-8")

    def deserialize(self, raw: bytes):
        return yaml.safe_load(raw.decode("utf-8"))

    def name_strategy(self, key: str) -> str:
        return f"{key}.yaml"


LOCAL_BASE_PATH = "./local_blob_storage"
LOCAL_CONTAINER = "test_container"

serializers = {
    "json": JSONSerializer(),
    "binary": BinarySerializer(),
    "yaml": YAMLSerializer(),
}


adapter = LocalFileAdapter(LOCAL_BASE_PATH)
container_name = LOCAL_CONTAINER

store = AsyncBlobStore(
    adapter,
    container_name,
    serializers=serializers,
    default_format="json",
)

# Now you can use:
await store.yaml.set("config", {"a": 1})
print(await store.yaml.get("config"))
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

Run only local tests:

```bash
pytest -m "not azure"
```

Run only Azure tests:

```bash
pytest -m azure
```

For code coverage HTML report:

```bash
pytest --cov=src --cov-report=html
```

---

## License

MIT License

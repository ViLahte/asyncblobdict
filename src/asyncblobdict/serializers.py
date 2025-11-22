import json
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID


class Serializer(Protocol):
    extension: str | None = None

    def serialize(self, obj: Any) -> bytes: ...
    def deserialize(self, data: bytes) -> Any: ...
    def name_strategy(self, key: str) -> str: ...


TYPE_MARKER = "_bdtype_"
JSONScalar = str | int | float | bool | datetime | UUID | None
JSONValue = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
ExtendedJSON = (
    JSONScalar | list["ExtendedJSON"] | dict[str, "ExtendedJSON"] | set["ExtendedJSON"]
)


def default_encoder(o: Any) -> Any:
    if isinstance(o, set):
        return {TYPE_MARKER: "set", "items": list(o)}
    if isinstance(o, datetime):
        return {TYPE_MARKER: "datetime", "value": o.isoformat()}
    if isinstance(o, UUID):
        return {TYPE_MARKER: "uuid", "value": str(o)}
    raise TypeError(f"Type {type(o)} not serializable")


def default_decoder(d: dict[str, Any]) -> Any:
    if TYPE_MARKER in d:
        if d[TYPE_MARKER] == "set":
            return set(d["items"])
        if d[TYPE_MARKER] == "datetime":
            return datetime.fromisoformat(d["value"])
        if d[TYPE_MARKER] == "uuid":
            return UUID(d["value"])
    return d


class JSONSerializer(Serializer):
    extension = "json"

    def serialize(self, obj: Any) -> bytes:
        try:
            return json.dumps(obj, default=default_encoder).encode("utf-8")
        except TypeError as e:
            raise ValueError(f"Value is not JSON-serializable: {e}")

    def deserialize(self, data: bytes) -> ExtendedJSON:
        try:
            return json.loads(data.decode("utf-8"), object_hook=default_decoder)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}")

    def name_strategy(self, key: str) -> str:
        return f"{key}.{self.extension}"


class BinarySerializer(Serializer):
    extension = None

    def serialize(self, obj: bytes) -> bytes:
        if not isinstance(obj, (bytes, bytearray)):
            raise ValueError("Binary data must be bytes or bytearray")
        return bytes(obj)

    def deserialize(self, data: bytes) -> bytes:
        return data

    def name_strategy(self, key: str) -> str:
        return key

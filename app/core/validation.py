import json

from pydantic import JsonValue

MAX_BODY_BYTES = 64 * 1024
MAX_METADATA_BYTES = 16 * 1024
MAX_JSON_DEPTH = 32


def validate_text(value: str) -> str:
    if "\x00" in value or any(0xD800 <= ord(char) <= 0xDFFF for char in value):
        raise ValueError("Text contains a character unsupported by PostgreSQL")
    return value


def validate_json(value: JsonValue, *, max_depth: int = MAX_JSON_DEPTH) -> None:
    pending: list[tuple[JsonValue, int]] = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if isinstance(item, (dict, list)):
            if depth > max_depth:
                raise ValueError(f"JSON nesting must not exceed {max_depth} levels")
            if isinstance(item, dict):
                for key, child in item.items():
                    validate_text(key)
                    pending.append((child, depth + 1))
            else:
                pending.extend((child, depth + 1) for child in item)
        elif isinstance(item, str):
            validate_text(item)


def validate_metadata(value: dict[str, JsonValue]) -> None:
    validate_json(value)
    if len(json.dumps(value, ensure_ascii=False).encode()) > MAX_METADATA_BYTES:
        raise ValueError("Metadata must not exceed 16 KiB when serialized as JSON")

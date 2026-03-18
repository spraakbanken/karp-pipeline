from typing import Any
import orjson

from pydantic import BaseModel

from karppipeline.common import Map


def custom_serializer(obj: object) -> object:
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    raise TypeError(f"Type {type(obj)} not serializable")


def dumps(obj: object, pretty=False) -> str:
    kwargs = {}
    if pretty:
        kwargs["option"] = orjson.OPT_INDENT_2
    return orjson.dumps(obj, default=custom_serializer, **kwargs).decode()


def loads(str: str) -> Map:
    return orjson.loads(str)


def load_array(data: bytes) -> list[Any]:
    return orjson.loads(data)

from typing import Any, BinaryIO, Final
import orjson

from pydantic import BaseModel

from karppipeline.common import Map


def custom_serializer(obj: object) -> object:
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    raise TypeError(f"Type {type(obj)} not serializable")


def dump(obj: object, fp: BinaryIO, pretty=False):
    fp.write(_dumps(obj, pretty=pretty))


def dumps(obj: object, pretty=False) -> str:
    return _dumps(obj, pretty).decode()


def load(fp: BinaryIO) -> Map:
    data = fp.read()
    return loads(data)


def loads(data: str | bytes) -> Map:
    return orjson.loads(data)


def load_array(data: bytes) -> list[Any]:
    return orjson.loads(data)


pretty_kwargs: Final[dict[str, int]] = {"option": orjson.OPT_INDENT_2}


def _dumps(obj: object, pretty=False) -> bytes:
    if pretty:
        return orjson.dumps(obj, default=custom_serializer, **pretty_kwargs)
    return orjson.dumps(obj, default=custom_serializer)


def dump_with_newline(obj: object, fp: BinaryIO) -> None:
    """
    Dump and write directory to fp, with a trailing newline (JSONL)
    """
    fp.write(orjson.dumps(obj, option=orjson.OPT_APPEND_NEWLINE))

import logging
from typing import Any, cast
from karppipeline.execution.dependency import Dependency
from karppipeline.models import Entry, PipelineConfig

from karppipeline.common import PipelineException, create_output_dir
from karppipeline.util import json

__all__ = ["export", "dependencies"]
logger = logging.getLogger(__name__)

dependencies = [Dependency("jsonl")]


def export(config: PipelineConfig, _, **_kwargs):
    """
    Writes each entry to file
    """

    def json_dump():
        with open(create_output_dir(config.workdir) / f"{config.resource_id}.flatter.jsonl", "w") as fp:
            flattened_entry = None
            while True:
                entry = yield flattened_entry
                if not entry:
                    break
                flattened_entry = flatten(entry)
                # if "sv" in entry and "pekare" in entry["sv"]:
                #     breakpoint()
                fp.write(json.dumps(flattened_entry) + "\n")
                

    gen = json_dump()
    next(gen)

    def task(entry: Entry | None, /) -> Entry | None:
        if not entry:
            return None
        logger.debug("jsonl entry task")
        new_entry = gen.send(entry)
        return new_entry

    return task


def flatten(entry: Entry) -> Entry:
    inner = recursive_flatten(cast(Any, entry))
    return dict(inner)


def recursive_flatten(
    val: dict[str, Any] | list[str | int | float | bool] | str | float | int | bool, collection_allowed=True
) -> list[tuple[str, dict[str, Any] | list[str | int | float | bool] | str | float | int | bool]]:
    res = []
    if isinstance(val, dict):
        for key, inner_val in val.items():
            inner = recursive_flatten(inner_val)
            for inner_key, item in inner:
                if inner_key:
                    new_key = f"{key}.{inner_key}"
                else:
                    new_key = key
                res.append((new_key, item))
    elif isinstance(val, list):
        if not collection_allowed:
            raise PipelineException("Collections in collections are not allowed")
        new_list = []
        for elem in val:
            inner_val = recursive_flatten(elem, collection_allowed=False)
            if isinstance(elem, dict):
                # lists with flat objects are allowed in this format
                new_list.append(dict(inner_val))
            else:
                # if inner_val was a list it would have crashed, 
                # [0] selects the first elem and
                # [1] throws away the key used in recursion
                new_list.append(inner_val[0][1])
        res.append((None, new_list))
    else:
        res.append((None, val))

    return res

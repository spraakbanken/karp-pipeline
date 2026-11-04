from copy import deepcopy
import importlib.util
import logging
from pathlib import Path
import pickle
from typing import Callable, Final, Iterable, cast
from karppipeline.execution.dependency import Dependency
from karppipeline.models import Entry, EntrySchema, PipelineConfig

from karppipeline.common import create_output_dir
from karppipeline.modules.datatransform.flatten import flatten_entry_schema, flatten_source_order, recursive_flatten

__all__ = ["export", "dependencies"]
logger = logging.getLogger(__name__)

dependencies = [Dependency("jsonl")]

MODULE_NAME: Final[str] = "datatransform"


def export(config: PipelineConfig, module_data, **_kwargs):

    entry_schema, source_order, data_transform_fun = _transform(
        config, module_data["schema"]["entry_schema"], module_data["schema"]["source_order"]
    )

    with open(_get_data_path(config), "wb") as fp:
        pickle.dump({"entry_schema": entry_schema, "source_order": source_order}, fp)

    def task(entry: Entry | None, /) -> list[Entry] | None:
        if not entry:
            return None
        logger.debug("jsonl entry task")
        new_entries = data_transform_fun(entry)
        return list(new_entries)

    return task


def _transform(
    config: PipelineConfig, input_entry_schema: EntrySchema, input_source_order: list[tuple[str, list]]
) -> tuple[EntrySchema, list[str], Callable[[Entry], Iterable[Entry]]]:
    """
    Check for resource transform plugins in <workdir>/plugins/ or fallback on the default transform which are
    the functions autoflatten, flatten_entryschema and flatten_source_order
    """

    def autoflatten(entry: Entry) -> Iterable[Entry]:
        inner = recursive_flatten(cast(dict, entry))
        yield dict(inner)

    # the default transform is flatten
    data_transform_fun: Callable[[Entry], Iterable[Entry]] = autoflatten
    schema_transform_fun = flatten_entry_schema
    source_order_transform_fun: Callable[[list[tuple[str, list]]], list[str]] = flatten_source_order

    # check if there is custom flatten script for resource
    functions = _load_user_defined_functions(config.plugin_dir)
    if functions:
        (data_transform_fun, schema_transform_fun, source_order_transform_fun) = functions

    new_schema = schema_transform_fun(deepcopy(input_entry_schema))
    new_source_order = source_order_transform_fun(input_source_order)
    return (new_schema, new_source_order, data_transform_fun)


def load(config) -> dict[str, object]:
    with open(_get_data_path(config), "rb") as fp:
        return pickle.load(fp)


def _load_user_defined_functions(
    plugin_dir: Path,
) -> (
    tuple[
        Callable[[Entry | None], Iterable[Entry]],
        Callable[[EntrySchema], EntrySchema],
        Callable[[list[tuple[str, list]]], list[str]],
    ]
    | None
):
    # check if there is a user-defined function to transform
    f = plugin_dir / "transform.py"
    if f.exists():
        spec = importlib.util.spec_from_file_location("plugin", f)
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            if hasattr(mod, "transform"):
                # transform must be defined
                data_flatten_fun = mod.transform

                def identity(x):
                    return x

                if hasattr(mod, "update_schema"):
                    schema_flatten_fun = mod.update_schema
                else:
                    schema_flatten_fun = identity
                if hasattr(mod, "update_source_order"):
                    source_order_flatten_fun = mod.update_source_order
                else:
                    source_order_flatten_fun = identity

                return data_flatten_fun, schema_flatten_fun, source_order_flatten_fun
    return None


def _get_data_path(config) -> Path:
    module_dir = create_output_dir(config.workdir) / MODULE_NAME
    module_dir.mkdir(exist_ok=True)
    return module_dir / "schema.pickle"

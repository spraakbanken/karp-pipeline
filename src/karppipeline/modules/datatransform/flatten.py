from typing import Any, Iterable
from karppipeline.common import PipelineException
from karppipeline.models import EntrySchema


def recursive_flatten(
    val: dict[str, Any] | list[str | int | float | bool] | str | float | int | bool, collection_allowed=True
) -> list[tuple[str, dict[str, Any] | list[str | int | float | bool] | str | float | int | bool]]:
    res = []
    if isinstance(val, dict):
        for key, inner_val in val.items():
            inner = recursive_flatten(inner_val, collection_allowed=collection_allowed)
            for inner_key, item in inner:
                if inner_key:
                    new_key = f"{key}.{inner_key}"
                else:
                    new_key = key
                res.append((new_key, item))
    elif isinstance(val, list):
        if not collection_allowed:
            raise PipelineException("Flat JSON (used for Karp-s): Collections in collections are not allowed")
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


def identity(x):
    return x


def flatten_entry_schema(entry_schema: EntrySchema):
    """
    Transforms entry schema into a "flat" schema
    """
    new_entry_schema = {}
    for key, outer_field in entry_schema.items():
        if outer_field.type == "object":
            inner_schema = flatten_entry_schema(outer_field.fields)
            for inner_key, field in inner_schema.items():
                new_name = key + "." + inner_key
                field.name = new_name
                new_entry_schema[new_name] = field
        else:
            new_entry_schema[key] = outer_field
    return new_entry_schema


def flatten_order_tree(fields: Iterable[tuple[str, Any]], target: list[str], path=""):
    for field_name, inner_fields in fields:
        if inner_fields:
            flatten_order_tree(inner_fields, target, path=path + field_name + ".")
        else:
            target.append(path + field_name)


def flatten_source_order(source_order: list[tuple[str, list]]) -> list[str]:
    flat_source_order = []
    flatten_order_tree(source_order, flat_source_order)
    return flat_source_order

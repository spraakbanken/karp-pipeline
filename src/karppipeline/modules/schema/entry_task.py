import importlib
import logging
from typing import Any, Iterator, Callable
import unicodedata
from karppipeline.models import EntrySchema, PipelineConfig, Entry, InferredField

logger = logging.getLogger(__name__)


def get_entry_converter(config: PipelineConfig, entry_schema: EntrySchema) -> Callable[[Entry | None], Entry | None]:
    """
    Check if config contains any renames or conversions
    Update the entry schema and each entry with this information
    """

    def _get_converter(converter: str) -> dict[str, Callable[[object], object]]:
        [module, func] = converter.split(".")
        mod = importlib.import_module("karppipeline.converters." + module)
        func_obj = getattr(mod, func)
        update_schema = getattr(mod, func + "_update_schema")
        return {"update_schema": update_schema, "convert": func_obj}

    add_all = False
    converted_fields = []
    converters = {}
    if len(config.export.fields) == 0:
        # add all the fields from the source to the target if there are no field settings
        add_all = True
    for field in config.export.fields:
        if field.root == "...":
            add_all = True
        else:
            converted_fields.append(field)
    if not add_all:
        entry_schema.clear()
    for field in converted_fields:
        if field.exclude:
            entry_schema.pop(field.name, None)
        else:
            if field.name == "*":
                # the converter must set the schema, placeholder value
                entry_schema[field.target] = InferredField(name=field.target, type="str")
            else:
                # TODO here we copy the schema from source field, but length may be different
                field_copy = entry_schema[field.name].copy()
                field_copy.name = field.target
                entry_schema[field.target] = field_copy
        # pre-import each converter
        if field.converter:
            converters[field.converter] = _get_converter(field.converter)
            entry_schema[field.target] = converters[field.converter]["update_schema"](entry_schema[field.target])

    def _convert_value(converter: str | None, val: Any) -> Any:
        if converter:
            return converters[converter]["convert"](config.resource_id, val)
        return val

    def convert(entry: Entry | None) -> Entry | None:
        if not entry:
            return None
        logger.debug("schema entry task")
        new_entry = {}

        # initialize data
        for key in entry_schema.keys():
            if key in entry:
                new_entry[key] = entry[key]

        # convert or rename fields
        for field in converted_fields:
            if not field.exclude:
                if field.name == "*":
                    val = _convert_value(field.converter, entry)
                    if val is not None:
                        new_entry[field.target] = val
                    else:
                        if field.target in new_entry:
                            del new_entry[field.target]
                else:
                    if field.name in new_entry:
                        val = new_entry[field.name]
                    else:
                        val = entry[field.name]
                    new_entry[field.target] = _convert_value(field.converter, val)

        # clean up all text fields
        for key in entry_schema.keys():
            if entry_schema[key].type == "text" and key in new_entry:
                if not entry_schema[key].collection:
                    new_entry[key] = _clean_text(new_entry[key]) if new_entry[key] else None
                else:
                    # this also causes all None to be []
                    new_entry[key] = [_clean_text(text) for text in new_entry[key] or []]

        return new_entry

    return convert


def _clean_text(text: str) -> str:
    """
    Removes control characters, formatting characters, unassigned characters and makes all spaces into "normal" space
    """

    def inner(text) -> Iterator[str]:
        for c in text:
            cat = unicodedata.category(c)
            if c == "\n":
                yield c
            # remove all control characters (Cc), formatting characters (Cf), unassigned characters(Cn)
            elif cat not in {"Cc", "Cf", "Cn"}:
                if cat == "Zs":
                    # normalize space separators
                    yield " "
                else:
                    yield c

    return "".join(inner(text))

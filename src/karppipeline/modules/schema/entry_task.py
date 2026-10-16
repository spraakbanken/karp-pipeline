import importlib
import importlib.util
import logging
from types import ModuleType
from typing import Any, Iterator, Callable, Self
import unicodedata
from karppipeline.common import PipelineException
from karppipeline.models import EntrySchema, PipelineConfig, Entry, InferredField

logger = logging.getLogger(__name__)


class ConverterImporter:
    """
    Config files can use converters for fields.

    If the converter Y is in karppipeline.converters.X, `export.fields` uses `old_field_name:X.Y as new_field_name`
    If the converter Y is in a plugin directory for the resource, `export.fields` uses `old_field_name:Y as new_field_name`

    The class makes sure that modules are only imported once and cashes some ready converters in case the same converter is
    used twice in the same resource.
    """

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.loaded_mods: list[ModuleType] = []
        self.converters: dict[str, dict[str, Callable]] = {}

    def get_converter(self: Self, converter: str) -> dict[str, Callable[[object], object]]:
        if converter in self.converters:
            return self.converters[converter]

        ref = converter.split(".")
        if len(ref) == 2:
            [module, func] = ref
            mod = importlib.import_module("karppipeline.converters." + module)
        else:
            if not self.loaded_mods:
                # plugin directories are inherited like configuration parents. load all the plugins found at this point.
                for plugin_file_path in self.config.plugin_files:
                    spec = importlib.util.spec_from_file_location("plugin", plugin_file_path)
                    if not spec or not spec.loader:
                        raise PipelineException(
                            "Resource is trying to use an unqualified function (X:Z as Y), but no code file was found for resource."
                        )
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)
                    self.loaded_mods.append(mod)

            func = ref[0]
            for mod in self.loaded_mods:
                if hasattr(mod, func):
                    break
            else:
                raise PipelineException(f"function {func} not found")
        func_obj = getattr(mod, func)

        update_schema = getattr(mod, func + "_update_schema")
        converter_obj = {"update_schema": update_schema, "convert": func_obj}
        self.converters[converter] = converter_obj
        return converter_obj


def get_entry_converter(config: PipelineConfig, entry_schema: EntrySchema) -> Callable[[Entry | None], Entry | None]:
    """
    Check if config contains any renames or conversions
    Update the entry schema and each entry with this information
    """
    converter_importer = ConverterImporter(config)

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
            # remove exluded field from schema (it might not be there)
            tmp: EntrySchema = entry_schema
            parts = field.name.split(".")
            aborted = False
            for part in parts[:-1]:
                thing = tmp.get(part)
                if thing:
                    tmp = thing.fields
                else:
                    aborted = True
                    break
            if not aborted:
                tmp.pop(parts[-1], None)
        else:
            if field.name == "*":
                # the converter must set the schema, placeholder value
                entry_schema[field.target] = InferredField(name=field.target, type="str")
            else:
                # TODO here we copy the schema from source field, but length may be different
                field_copy = entry_schema[field.name].copy()
                field_copy.name = field.target

                # check if target field is configured as categorical
                categories = None
                for conf_field in config.fields:
                    if conf_field.name == field.target and conf_field.categorical:
                        categories = set(conf_field.categories)
                        break

                # we must set categories to *not* None for the pipeline to understand that the field is categorical
                if not field_copy.categories:
                    field_copy.categories = categories

                entry_schema[field.target] = field_copy
        # pre-import each converter
        if field.converter:
            converters[field.converter] = converter_importer.get_converter(field.converter)
            entry_schema[field.target] = converters[field.converter]["update_schema"](entry_schema[field.target])

    def _convert_value(converter: str | None, val: Any) -> Any:
        if converter:
            return converters[converter]["convert"](config.resource_id, val)
        return val

    categorical_fields = {field.name: field for field in config.fields if field.categories}

    def convert(entry: Entry | None) -> Entry | None:
        if not entry:
            return None
        logger.debug("schema entry task")

        def init(entry_schema, entry) -> dict[str, Any]:
            new_entry = {}
            for key in entry_schema.keys():
                if key in entry:
                    if entry_schema[key].type == "object":
                        inner_obj = entry[key]
                        if not isinstance(inner_obj, list):
                            new_val = init(entry_schema[key].fields, inner_obj)
                        else:
                            new_val = [init(entry_schema[key].fields, elem) for elem in inner_obj]
                    else:
                        new_val = entry[key]
                    new_entry[key] = new_val
            return new_entry

        new_entry = init(entry_schema, entry)

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
                    val = None
                    if field.name in new_entry:
                        val = new_entry[field.name]
                    elif field.name in entry:
                        val = entry[field.name]
                    if val is not None:
                        new_entry[field.target] = _convert_value(field.converter, val)

        for key in entry_schema.keys():
            # clean up all text fields
            if entry_schema[key].type == "text" and key in new_entry:
                if not entry_schema[key].collection:
                    new_entry[key] = _clean_text(new_entry[key]) if new_entry[key] is not None else None
                else:
                    # this also causes all None to be []
                    new_entry[key] = [_clean_text(text) for text in new_entry[key] or []]

            # check that categorical types use allowed values
            if key in categorical_fields:
                error = False
                field = categorical_fields[key]
                val = new_entry[key]
                if field.collection:
                    for val in new_entry[key]:
                        if val not in field.categories:
                            error = True
                            break
                else:
                    if val not in field.categories:
                        error = True
                if error:
                    raise PipelineException(
                        f"{val} not a valid category in field {key} (Allowed: {', '.join(field.categories)})"
                    )

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

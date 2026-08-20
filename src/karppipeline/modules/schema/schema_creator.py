from typing import Iterator, cast
from karppipeline.common import PipelineException
from karppipeline.models import EntrySchema, PipelineConfig, Entry, InferredField
from karppipeline.models import ConfiguredField

from karppipeline.read import read_data

type_lookup: dict[type, str] = {int: "integer", str: "text", bool: "bool", float: "float"}


def pre_import_resource(pipeline_config: PipelineConfig) -> tuple[EntrySchema, list[str], list[int]]:
    """
    reads source file and generates a schema, return (source order, size of resource, schema)
    source order is roughly the order that fields occur in source file
    """
    source_order, size, entries = read_data(pipeline_config)

    # generate schema from entries - _create_field will exaust the generator and make size updated
    entry_schema = _create_fields(pipeline_config, entries)

    return (entry_schema, source_order, size)


def _create_fields(pipeline_config: PipelineConfig, entries: Iterator[Entry]) -> EntrySchema:
    """
    Goes through the entries and each key in the entries and populates schema
    """
    schema = {}
    for idx, entry in enumerate(entries):
        for key in entry:
            values = entry[key]
            try:
                _check_or_create_field(pipeline_config, schema, key, values)
            except PipelineException as e:
                raise PipelineException(f"Error for entry number: {idx + 1}: " + e.args[0])
    return schema


def _check_or_create_field(pipeline_config: PipelineConfig, schema, key, values):
    """
    Called for each key and value in each entry

    For unknown fields, initializes the field, for known fields, check that the given values
    match the field.
    """
    field: InferredField = schema.get(key)

    collection = False
    if not isinstance(values, list):
        values = (values,)
    elif field and not field.collection:
        raise PipelineException(f'Mismatch, field: "{key}". Pipeline does not except a collection.')
    else:
        collection = True
    for value in values:
        if not isinstance(value, dict):
            if value is None:
                break
            if isinstance(value, list):
                # at this point, inner_value must be scalar otherwise the source file's entry schema is not supported
                # TODO allow it...
                raise PipelineException("Level of nesting not allowed.")
            if field:
                _check_type(key, field, value)

            else:
                # not previously seen field, initializes type and name
                conf_field = pipeline_config.get_field(key)
                categorical = False
                if conf_field:
                    categorical = conf_field.categorical
                field = InferredField(
                    type=type_lookup[type(value)], name=key, collection=collection, categorical=categorical
                )
                schema[key] = field

            if field and field.type == "text":
                _add_max_length(field, value)

        else:
            if not field:
                # first time this table field is found
                fields = {}
                field = InferredField(type="object", collection=collection, name=key, fields=fields)
                schema[key] = field

            for inner_key, val in value.items():
                # use fields from the object field as schema, will add sub-fields to the correct level
                _check_or_create_field(pipeline_config, schema[key].fields, inner_key, val)


def _check_type(key: str, field: InferredField, value: str | float | int | bool) -> None:
    field_type = field.type
    actual_type_name = type_lookup[type(value)]
    # it is fine to first infer float and then seeing integer values
    if not (actual_type_name == "integer" and field_type == "float") and field_type != actual_type_name:
        raise PipelineException(f'Mismatch, field: "{key}". Was {actual_type_name}, expected {field_type}.')


def _add_max_length(field: InferredField, value: str):
    """
    Sets or update the longest value seen for this field, only works for text fields
    """
    field.extra["length"] = max(cast(int, field.extra.get("length", 0)), len(value))


def compare_to_configured_fields(config: PipelineConfig, entry_schema: EntrySchema):
    """
    Check that the inferred types are the same as the configured type.
    """

    def to_dict(elems: list[ConfiguredField]) -> dict[str, ConfiguredField]:
        return {elem.name: elem for elem in elems}

    config_fields: dict[str, ConfiguredField] = to_dict(config.fields)
    for key, field in entry_schema.items():
        field: InferredField
        if key in config_fields:
            config_field = config_fields[key]
            error = False
            if config_field.collection != field.collection:
                error = True
            elif config_field.type != field.type:
                error = True

            if error:
                raise PipelineException(
                    f"{key} is configured, but it is not the same as in this resource, must rename."
                )

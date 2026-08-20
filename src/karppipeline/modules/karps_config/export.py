import time
from typing import Any, Callable, Iterable, Iterator, Mapping


from karppipeline.common import PipelineException, create_output_dir
from karppipeline.modules.karps.models import KarpsExportConfig
from karppipeline.models import ConfiguredField, EntrySchema, PipelineConfig
from karppipeline.util import yaml


def identity(x):
    return x


def flatten_entry_schema(entry_schema: EntrySchema, field_collector: Callable = identity):
    """
    Transforms entry schema into a "flat" schema
    """
    new_entry_schema = {}
    for key, outer_field in entry_schema.items():
        if outer_field.type == "object":
            inner_schema = flatten_entry_schema(field_collector(outer_field.fields), field_collector)
            for inner_key, field in inner_schema.items():
                new_name = key + "." + inner_key
                field.name = new_name
                new_entry_schema[new_name] = field
        else:
            new_entry_schema[key] = outer_field
    return new_entry_schema


def flatten_pipeline_fields(fields) -> dict[str, ConfiguredField]:
    def field_collector(fields):
        return {field.name: field for field in fields}

    return flatten_entry_schema(field_collector(fields), field_collector)


def create_karps_backend_config(
    pipeline_config: PipelineConfig,
    karps_config: KarpsExportConfig,
    name: dict[str, str],
    description: dict[str, str],
    entry_schema: EntrySchema,
    source_order,  # TODO type
    size: int,
):

    fields: list[dict[str, object]] = []
    # collected_categories = module_data["generate_categorical_values"]
    configured_fields = flatten_pipeline_fields(pipeline_config.fields)
    for field in entry_schema.values():
        field_dict = field.asdict()
        # TODO make sure this works for sub-fields
        if field.name in configured_fields:
            conf_field = configured_fields[field.name]
            if conf_field.label:
                field_dict["label"] = conf_field.label.model_dump()
            if conf_field.categorical:
                field_dict["categories"] = conf_field.categories  # or collected_categories[conf_field.name]
            if conf_field.category_labels:
                field_dict["category_labels"] = {
                    category: category_label.model_dump()
                    for category, category_label in conf_field.category_labels.items()
                }

        if "label" not in field_dict:
            if pipeline_config.protected_metadata:
                field_dict["label"] = field.name.split(f"_{pipeline_config.resource_id}_")[1]
            else:
                field_dict["label"] = field.name

        if pipeline_config.protected_metadata:
            field_dict["protected_metadata"] = True

        fields.append(field_dict)

    karps_workdir = create_output_dir(pipeline_config.workdir) / "karps"

    # these fields might already be present in backend config, install must merge this file and backend fields.yaml
    with open(karps_workdir / "fields.yaml", "w") as fp:
        yaml.dump(fields, fp)

    with open(karps_workdir / "global.yaml", "w") as fp:
        yaml.dump(
            {"tags_description": {key: val.model_dump() for key, val in karps_config.tags_description.items()}}, fp
        )

    def order_fields(fields: list[str]) -> Iterable[str]:
        flatten_pipeline_fields = []
        flattened_source_order = []

        def flatten_field_conf(fields: Iterable[ConfiguredField], target: list[str]):
            """
            For some reason, ConfiguredField's already have collapsed their names at this point
            """
            for field in fields:
                if field.type == "object":
                    flatten_field_conf(field.fields, target)
                else:
                    target.append(field.name)

        def flatten_order_tree(fields: Iterable[tuple[str, Any]], target: list[str], path=""):
            for field_name, inner_fields in fields:
                if inner_fields:
                    flatten_order_tree(inner_fields, target, path=field_name + ".")
                else:
                    target.append(path + field_name)

        flatten_field_conf(pipeline_config.fields, flatten_pipeline_fields)
        flatten_order_tree(source_order, flattened_source_order)

        # initialize main sort order
        order_map = {name: i for i, name in enumerate(flatten_pipeline_fields)}

        # order by apperance in input objects for non-configured fields
        for i, name in enumerate(flattened_source_order):
            if name not in order_map:
                order_map[name] = len(flatten_pipeline_fields) + i

        # should be no unknown fields at this point (TODO: not true, because generated fields are not in source_order)
        sorted_keys = sorted(fields, key=lambda x: order_map[x])
        return sorted_keys

    def make_field_config(fields: Iterable[str]) -> Iterator[Mapping[str, object]]:
        """
        creates the final format for a field in karps config
        if only one of karps.primary/secondary is given:
            for each key in karps_config.primary, add primary: true and primary: false to the rest
            for each key in karps_config.secondary, add primary: false and primary: true to the rest
        else:
            add primary: true/false as expected and raise error if a field is not in either
        """
        primary = karps_config.primary
        secondary = karps_config.secondary
        for field in fields:
            if primary and secondary:
                if not (field in primary or field in secondary):
                    raise Exception(
                        f'Karps: field {field} has to be in either primary or secondary. Use "not {field}" in export.fields to exclude field or update primary/secondary.'
                    )
                is_primary = field in primary
            elif karps_config.primary:
                is_primary = field in primary
            elif karps_config.secondary:
                is_primary = field not in secondary
            else:
                # if primary/secondary is not configured, all fields are primary
                is_primary = True
            yield {"name": field, "primary": is_primary}

    final_field_list = order_fields(list(entry_schema.keys()))
    backend_config = {
        "resource_id": pipeline_config.resource_id,
        "label": name or pipeline_config.resource_id,
        "fields": list(make_field_config(final_field_list)),
        "entry_word": karps_config.entry_word.model_dump(),
        "size": size,
        "link": karps_config.link,
        "updated": int(time.time()),
    }
    if karps_config.entry_word.field not in final_field_list:
        raise PipelineException(
            f"entry_word: {karps_config.entry_word.field}, but field is not available in the resource"
        )
    if karps_config.tags:
        backend_config["tags"] = karps_config.tags
    backend_config["description"] = description
    if pipeline_config.limited_access:
        backend_config["limited_access"] = pipeline_config.limited_access
    if pipeline_config.protected_metadata:
        backend_config["protected_metadata"] = pipeline_config.protected_metadata

    with open(karps_workdir / "resource.yaml", "w") as fp:
        yaml.dump(backend_config, fp)

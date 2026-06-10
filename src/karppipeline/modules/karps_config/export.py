import time
from typing import Iterable, Iterator, Mapping


from karppipeline.common import PipelineException, create_output_dir
from karppipeline.modules.karps.models import KarpsExportConfig
from karppipeline.models import EntrySchema, PipelineConfig
from karppipeline.util import yaml


def create_karps_backend_config(
    pipeline_config: PipelineConfig,
    karps_config: KarpsExportConfig,
    name: dict[str, str],
    description: dict[str, str],
    entry_schema: EntrySchema,
    source_order: list[str],
    size: int,
):
    fields: list[dict[str, object]] = []
    # collected_categories = module_data["generate_categorical_values"]
    configured_fields = {field.name: field for field in pipeline_config.fields}
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

    def order_fields(fields: Iterator[str]) -> Iterable[str]:
        # initialize main sort order
        order_map = {name: i for i, name in enumerate([field.name for field in pipeline_config.fields])}

        # order by apperance in input objects for non-configured fields
        for i, name in enumerate(source_order):
            if name not in order_map:
                order_map[name] = len(pipeline_config.fields) + i

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

    final_field_list = order_fields(iter(entry_schema.keys()))
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

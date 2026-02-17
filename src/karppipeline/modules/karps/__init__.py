import logging
from typing import Callable

from karppipeline.common import ImportException, create_output_dir
import karppipeline.modules.karps.install as backend_install
from karppipeline.modules.karps.models import KarpsConfig
from karppipeline.models import ConfiguredField, Entry, EntrySchema, InferredField, PipelineConfig
import karppipeline.modules.karps.export as backend_export
from karppipeline.run import Dependency

"""
generate Karp-s backend configuration and SQL, could be broken up into two tasks
"""

__all__ = ["export", "install", "dependencies"]
logger = logging.getLogger(__name__)


dependencies = [Dependency("sbxmetadata", optional=True), Dependency("schema"), Dependency("jsonl")]


def export(
    config: PipelineConfig,
    module_data,
) -> list[Callable[[Entry], Entry]]:
    """
    Create configuration and SQL data file for Karp-s backend
    """
    entry_schema: EntrySchema = module_data["schema"]["entry_schema"]
    source_order: list[str] = module_data["schema"]["source_order"]
    size: int = module_data["schema"]["size"]

    fields: list[dict[str, str]] = _compare_to_current_fields(config, entry_schema)
    create_output_dir(config.workdir)
    module_config = _get_module_config(config)

    # sql_gen is a coroutine for creating the SQL file for backend
    sql_gen = backend_export.create_karps_sql(config, module_config, entry_schema)

    # fallback value because sbxmetadata is optional
    sbxmetadata = module_data["sbxmetadata"] or {}
    name = sbxmetadata.get("name") or config.name and config.name.model_dump()
    if not name:
        raise ImportException("karps: 'name' missing")
    backend_export.create_karps_backend_config(config, module_config, name, entry_schema, source_order, size, fields)

    next(sql_gen)

    def task(entry: Entry) -> Entry:
        logger.debug("karps entry task")
        sql_gen.send(entry)
        return entry

    return [task]


def install(pipeline_config: PipelineConfig):
    """
    1. Move Karp-s backend configuration file to the configured backend configuration directory.
    2. Run the SQL file in the configured database.
    """
    karps_config = _get_module_config(pipeline_config)

    backend_install.add_to_db(pipeline_config, karps_config)
    backend_install.add_config(pipeline_config, karps_config, pipeline_config.resource_id)


def _get_module_config(config):
    return KarpsConfig.model_validate(config.modules["karps"])


def _compare_to_current_fields(config: PipelineConfig, entry_schema: EntrySchema) -> list[dict[str, str]]:
    """
    Looks in the main config file for presets about the fields, mainly label but could also be tagset
    """

    def to_dict(elems: list[ConfiguredField]) -> dict[str, ConfiguredField]:
        return {elem.name: elem for elem in elems}

    main_fields: dict[str, ConfiguredField] = to_dict(config.fields)
    new_fields = []
    for key, field in entry_schema.items():
        field: InferredField
        if key in main_fields:
            main_field = main_fields[key]
            # TODO other settings, like values for enums are not taken into account
            if main_field.collection != field.collection or main_field.type != field.type:
                raise ImportError(
                    f"{key} is configured, but it is not the same as in this resource, must rename or add alias."
                )
            new_fields.append(main_field.model_dump(exclude_unset=True))
        else:
            new_field = field.asdict()
            new_fields.append(new_field)
    return new_fields

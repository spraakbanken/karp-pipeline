import logging
from typing import Callable, Final

from karppipeline.common import PipelineException, create_output_dir
from karppipeline.execution.dependency import Dependency
import karppipeline.modules.karps.install as backend_install
from karppipeline.modules.karps.models import KarpsExportConfig, KarpsInstallConfig
from karppipeline.models import Entry, EntrySchema, MultiLang, PipelineConfig
import karppipeline.modules.karps.export as backend_export

"""
generate Karp-s backend configuration and SQL, could be broken up into two tasks
"""

__all__ = ["export", "install", "dependencies"]
logger = logging.getLogger(__name__)

MODULE_NAME: Final[str] = "karps"

dependencies = [
    Dependency("sbxmetadata", optional=True),
    Dependency("schema"),
    Dependency("jsonl"),
    Dependency("generate_categorical_values"),
]


def _add_namespace_to_schema(namespace: str, schema: EntrySchema) -> EntrySchema:
    new_schema = {}
    for key, val in schema.items():
        new_key = _add_namespace_to_field(namespace, key)
        val.name = new_key
        new_schema[new_key] = val
    return new_schema


def _add_namespace_to_fields(namespace, fields: list[str]) -> list[str]:
    return [_add_namespace_to_field(namespace, field) for field in fields]


def _add_namespace_to_field(namespace: str, field: str) -> str:
    return "_" + namespace + "_" + field


def export(config: PipelineConfig, module_data, instance: str = MODULE_NAME) -> Callable[[Entry | None], Entry | None]:
    """
    Create configuration and SQL data file for Karp-s backend
    """
    entry_schema: EntrySchema = module_data["schema"]["entry_schema"]
    source_order: list[str] = module_data["schema"]["source_order"]
    size: int = module_data["schema"]["size"]

    create_output_dir(config.workdir)
    karps_workdir = create_output_dir(config.workdir) / "karps"
    karps_workdir.mkdir(exist_ok=True)

    module_config = _get_export_config(config, instance)

    if config.protected_metadata:
        # if a resource uses protected_metadata: true, we will use the resource ID as a namespace
        modified_entry_schema = _add_namespace_to_schema(config.resource_id, entry_schema)
        modified_source_order = _add_namespace_to_fields(config.resource_id, source_order)

        # update entry_word, primary and secondary
        module_config.entry_word.field = _add_namespace_to_field(config.resource_id, module_config.entry_word.field)
        module_config.primary = _add_namespace_to_fields(config.resource_id, module_config.primary)
        module_config.secondary = _add_namespace_to_fields(config.resource_id, module_config.secondary)
    else:
        modified_entry_schema = entry_schema
        modified_source_order = source_order

    # sql_gen is a coroutine for creating the SQL file for backend
    sql_gen = backend_export.create_karps_sql(config, module_config, modified_entry_schema)

    # fallback value because sbxmetadata is optional
    sbxmetadata = module_data["sbxmetadata"] or {}
    name = sbxmetadata.get("name") or config.name and config.name.model_dump()
    if not name:
        raise PipelineException("karps: 'name' missing")

    description = (
        sbxmetadata.get("short_description")
        or sbxmetadata.get("description")
        or config.description
        and config.description.model_dump()
    )
    if not description and not config.allow_empty_description:
        raise PipelineException("karps: 'description' missing")
    elif not description:
        description = MultiLang.create("").model_dump()

    next(sql_gen)

    def task(entry: Entry | None) -> Entry | None:
        nonlocal sql_gen
        logger.debug("karps entry task")
        try:
            new_entry = entry
            if entry and config.protected_metadata:
                new_entry = {}
                for key, val in entry.items():
                    new_entry[_add_namespace_to_field(config.resource_id, key)] = val

            sql_gen.send(new_entry)
        except StopIteration:
            # if this happens, the entries are exhausted
            sql_gen = None
        if entry is None:
            backend_export.create_karps_backend_config(
                config, module_config, name, description, modified_entry_schema, modified_source_order, size
            )
        return entry

    return task


def install(pipeline_config: PipelineConfig, uninstall=False, instance=MODULE_NAME):
    """
    1. Move Karp-s backend configuration file to the configured backend configuration directory.
    2. Run the SQL file in the configured database.
    """
    karps_config = _get_install_config(pipeline_config, instance)
    if not uninstall:
        backend_install.add_to_db(pipeline_config, karps_config)
        backend_install.add_config(pipeline_config, karps_config, pipeline_config.resource_id)
    else:
        backend_install.remove_from_db(pipeline_config, karps_config)
        backend_install.remove_config(karps_config, pipeline_config.resource_id)


def _get_export_config(config, instance):
    return KarpsExportConfig.model_validate(config.modules[instance])


def _get_install_config(config, instance):
    return KarpsInstallConfig.model_validate(config.modules[instance])

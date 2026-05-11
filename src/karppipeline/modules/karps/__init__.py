import logging
from typing import Callable

from karppipeline.common import PipelineException, create_output_dir
from karppipeline.execution.dependency import Dependency
import karppipeline.modules.karps.install as backend_install
from karppipeline.modules.karps.models import KarpsConfig
from karppipeline.models import Entry, EntrySchema, PipelineConfig
import karppipeline.modules.karps.export as backend_export

"""
generate Karp-s backend configuration and SQL, could be broken up into two tasks
"""

__all__ = ["export", "install", "dependencies"]
logger = logging.getLogger(__name__)


dependencies = [
    Dependency("sbxmetadata", optional=True),
    Dependency("schema"),
    Dependency("jsonl"),
    Dependency("generate_categorical_values"),
]


def export(
    config: PipelineConfig,
    module_data,
) -> list[Callable[[Entry | None], Entry | None]]:
    """
    Create configuration and SQL data file for Karp-s backend
    """
    entry_schema: EntrySchema = module_data["schema"]["entry_schema"]
    source_order: list[str] = module_data["schema"]["source_order"]
    size: int = module_data["schema"]["size"]

    create_output_dir(config.workdir)
    karps_workdir = create_output_dir(config.workdir) / "karps"
    karps_workdir.mkdir(exist_ok=True)

    module_config = _get_module_config(config)

    # sql_gen is a coroutine for creating the SQL file for backend
    sql_gen = backend_export.create_karps_sql(config, module_config, entry_schema)

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
    if not description:
        raise PipelineException("karps: 'description' missing")

    config_gen = backend_export.create_karps_backend_config(
        config, module_config, name, description, entry_schema, source_order, size
    )

    next(sql_gen)
    next(config_gen)

    def task(entry: Entry | None) -> Entry | None:
        nonlocal sql_gen
        nonlocal config_gen
        logger.debug("karps entry task")
        try:
            sql_gen.send(entry)
        except StopIteration:
            # if this happens, the entries are exhausted
            sql_gen = None
        try:
            config_gen.send(entry)
        except StopIteration:
            # if this happens, the entries are exhausted
            config_gen = None
        return entry

    return [task]


def install(pipeline_config: PipelineConfig, uninstall=False):
    """
    1. Move Karp-s backend configuration file to the configured backend configuration directory.
    2. Run the SQL file in the configured database.
    """
    karps_config = _get_module_config(pipeline_config)
    if not uninstall:
        backend_install.add_to_db(pipeline_config, karps_config)
        backend_install.add_config(pipeline_config, karps_config, pipeline_config.resource_id)
    else:
        backend_install.remove_from_db(pipeline_config, karps_config)
        backend_install.remove_config(karps_config, pipeline_config.resource_id)


def _get_module_config(config):
    return KarpsConfig.model_validate(config.modules["karps"])

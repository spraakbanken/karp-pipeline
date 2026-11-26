import logging
from typing import Callable, Final

from karppipeline.common import PipelineException, create_output_dir
from karppipeline.execution.dependency import Dependency
from karppipeline.modules import datatransform
import karppipeline.modules.karps_config.install as backend_install
from karppipeline.modules.karps.models import (
    KarpsExportConfig,
    get_export_config,
    get_install_config,
)
from karppipeline.models import Entry, EntrySchema, MultiLang, PipelineConfig
import karppipeline.modules.karps_config.export as backend_export
from karppipeline.modules.schema.entry_task import get_entry_converter

"""
generate Karp-s backend configuration
"""

__all__ = ["export", "install", "dependencies"]
logger = logging.getLogger(__name__)

MODULE_NAME: Final[str] = "karps"
# simplified schema without settings for hosts, paths etc.
USER_MODEL: Final[type[KarpsExportConfig]] = KarpsExportConfig

dependencies = [
    Dependency("sbxmetadata", optional=True),
    Dependency(datatransform.MODULE_NAME),
]


def export(config: PipelineConfig, module_data, instance: str = MODULE_NAME) -> Callable[[Entry | None], Entry | None]:
    """
    Create configuration for Karp-s backend
    """
    entry_schema: EntrySchema = module_data[datatransform.MODULE_NAME]["entry_schema"]
    source_order: list[str] = module_data[datatransform.MODULE_NAME]["source_order"]

    create_output_dir(config.workdir)
    karps_workdir = create_output_dir(config.workdir) / "karps"
    karps_workdir.mkdir(exist_ok=True)

    module_config = get_export_config(config, instance)

    # updates entry_schema in place
    entry_converter = get_entry_converter(config, module_config.export_fields, entry_schema)

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

    karps_size = [0]

    def task(entry: Entry | None) -> Entry | None:
        """
        when all entries are processed, create the backend config
        """
        logger.debug("karps entry task")
        if entry is None:
            backend_export.create_karps_backend_config(
                config, module_config, name, description, entry_schema, source_order, karps_size[0]
            )
            return None
        karps_size[0] += 1
        return entry_converter(entry)

    return task


def install(pipeline_config: PipelineConfig, uninstall=False, instance=MODULE_NAME):
    """
    1. Move Karp-s backend configuration file to the configured backend configuration directory.
    2. Run the SQL file in the configured database.
    """
    karps_config = get_install_config(pipeline_config, instance)
    logger.info(f"Installing/uninstalling in Karp-s instance with name: '{instance}'")
    if not uninstall:
        backend_install.add_config(pipeline_config, karps_config, pipeline_config.resource_id)
    else:
        backend_install.remove_config(karps_config, pipeline_config.resource_id)

import logging
import pickle
from typing import Callable, Final

from karppipeline.common import PipelineException, create_output_dir
from karppipeline.execution.dependency import Dependency
import karppipeline.modules.karps_config.install as backend_install
from karppipeline.modules.karps.models import (
    get_export_config,
    get_install_config,
)
from karppipeline.models import Entry, EntrySchema, MultiLang, PipelineConfig
import karppipeline.modules.karps_config.export as backend_export
from karppipeline.modules.karps.namespace import add_namespace_to_fields, add_namespace_to_schema

"""
generate Karp-s backend configuration
"""

__all__ = ["export", "install", "dependencies"]
logger = logging.getLogger(__name__)

MODULE_NAME: Final[str] = "karps"

dependencies = [
    Dependency("sbxmetadata", optional=True),
    Dependency("schema"),
]


def export(config: PipelineConfig, module_data, instance: str = MODULE_NAME) -> Callable[[Entry | None], Entry | None]:
    """
    Create configuration for Karp-s backend
    """
    entry_schema: EntrySchema = module_data["schema"]["entry_schema"]
    source_order: list[str] = module_data["schema"]["source_order"]
    size: int = module_data["schema"]["size"]

    create_output_dir(config.workdir)
    karps_workdir = create_output_dir(config.workdir) / "karps"
    karps_workdir.mkdir(exist_ok=True)

    module_config = get_export_config(config, instance)

    if config.protected_metadata:
        # if a resource uses protected_metadata: true, we will use the resource ID as a namespace
        modified_entry_schema = add_namespace_to_schema(config.resource_id, entry_schema)
        modified_source_order = add_namespace_to_fields(config.resource_id, source_order)
    else:
        modified_entry_schema = entry_schema
        modified_source_order = source_order

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

    def task(entry: Entry | None) -> Entry | None:
        """
        when all entries are processed, create the backend config
        """
        logger.debug("karps entry task")
        if entry is None:
            backend_export.create_karps_backend_config(
                config, module_config, name, description, modified_entry_schema, modified_source_order, size
            )
        return entry

    # save the maybe namespaced entry schema for usage in karps_data
    with open(_get_data_path(config), "wb") as fp:
        pickle.dump(modified_entry_schema, fp)

    return task


def load(config) -> dict[str, object]:
    with open(_get_data_path(config), "rb") as fp:
        return pickle.load(fp)


def _get_data_path(config: PipelineConfig):
    return create_output_dir(config.workdir) / "karps/schema.pickle"


def install(pipeline_config: PipelineConfig, uninstall=False, instance=MODULE_NAME):
    """
    1. Move Karp-s backend configuration file to the configured backend configuration directory.
    2. Run the SQL file in the configured database.
    """
    karps_config = get_install_config(pipeline_config, instance)
    if not uninstall:
        backend_install.add_config(pipeline_config, karps_config, pipeline_config.resource_id)
    else:
        backend_install.remove_config(karps_config, pipeline_config.resource_id)

import logging
from typing import Callable, Final

from karppipeline.common import create_output_dir
from karppipeline.execution.dependency import Dependency
import karppipeline.modules.karps_data.install as backend_install
from karppipeline.modules.karps.models import (
    get_export_config,
    get_install_config,
)
from karppipeline.models import Entry, EntrySchema, PipelineConfig
import karppipeline.modules.karps_data.export as backend_export
from karppipeline.modules.karps.namespace import add_namespace_to_field

"""
generate Karp-s backend configuration and SQL, could be broken up into two tasks
"""

__all__ = ["export", "install", "dependencies"]
logger = logging.getLogger(__name__)

MODULE_NAME: Final[str] = "karps"

dependencies = [
    Dependency("karps_config"),
    Dependency("jsonl"),
    Dependency("generate_categorical_values"),
]


def export(config: PipelineConfig, module_data, instance: str = MODULE_NAME) -> Callable[[Entry | None], Entry | None]:
    """
    Create configuration and SQL data file for Karp-s backend
    """
    # use entry_schema from karps_config and not schema module
    entry_schema: EntrySchema = module_data["karps_config"]

    create_output_dir(config.workdir)
    karps_workdir = create_output_dir(config.workdir) / "karps"
    karps_workdir.mkdir(exist_ok=True)

    module_config = get_export_config(config, instance)

    # sql_gen is a coroutine for creating the SQL file for backend
    sql_gen = backend_export.create_karps_sql(config, module_config, entry_schema)

    next(sql_gen)

    def task(entry: Entry | None) -> Entry | None:
        nonlocal sql_gen
        logger.debug("karps entry task")
        try:
            new_entry = entry
            if entry and config.protected_metadata:
                new_entry = {}
                for key, val in entry.items():
                    new_entry[add_namespace_to_field(config.resource_id, key)] = val

            sql_gen.send(new_entry)
        except StopIteration:
            # if this happens, the entries are exhausted
            sql_gen = None
        return entry

    return task


def install(pipeline_config: PipelineConfig, uninstall=False, instance=MODULE_NAME):
    """
    1. Move Karp-s backend configuration file to the configured backend configuration directory.
    2. Run the SQL file in the configured database.
    """
    karps_config = get_install_config(pipeline_config, instance)
    if not uninstall:
        backend_install.add_to_db(pipeline_config, karps_config)
    else:
        backend_install.remove_from_db(pipeline_config, karps_config)

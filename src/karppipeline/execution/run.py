import logging
from typing import Callable


from karppipeline.execution.dependency import resolve_commands
from karppipeline.read import read_data

from karppipeline.models import Entry, PipelineConfig


logger = logging.getLogger(__name__)


def run(config: PipelineConfig, subcommand: list[str] | None = None) -> None:
    resolved_cmds, mods = resolve_commands(config, subcommand, config.export.default)

    entry_tasks: list[Callable[[Entry | None], Entry]] = []
    module_data = {}
    for cmd in resolved_cmds:
        mod = mods[cmd]
        dependencies = mod.dependencies
        for dependency in dependencies:
            dependency_name = dependency.name
            if dependency_name not in module_data:
                # fetch the result from cmd's dependency
                if dependency_name in mods and hasattr(mods[dependency_name], "load"):
                    module_data[dependency_name] = mods[dependency_name].load(config)
                else:
                    # add dependency so we don't have to look for the load method again
                    module_data[dependency_name] = None
        entry_task = mod.export(config, module_data, instance=cmd)

        # callables added to entry_tasks will be called for each entry
        if entry_task:
            entry_tasks.append(entry_task)

    # for each entry, do the needed tasks
    for entry in read_data(config)[2]:
        updated_entry = entry
        for task in entry_tasks:
            updated_entry = task(updated_entry)

    # sending None to all entry tasks to signal that no more entries are coming
    for task in entry_tasks:
        task(None)

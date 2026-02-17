from dataclasses import dataclass
import importlib
import logging
from typing import Callable

from karppipeline.common import ImportException
from karppipeline.read import read_data

from karppipeline.models import Entry, PipelineConfig


logger = logging.getLogger(__name__)


@dataclass
class Dependency:
    name: str
    optional: bool = False


def run(config: PipelineConfig, subcommand: list[str] | None = None) -> None:
    if subcommand is None:
        invoked_cmds = config.export.default
    else:
        invoked_cmds = subcommand

    resolved_cmds = []
    mods = {}

    def resolve(invoked_cmds):
        """
        Traverses the dependency tree and adds dependencies to resolved_cmds in the order they need to run
        """
        for cmd in invoked_cmds:
            try:
                mod = importlib.import_module("karppipeline.modules." + cmd)
                mods[cmd] = mod
            except ModuleNotFoundError as e:
                raise ImportException(f"{cmd} not found") from e
            # only add optional dependencies if they are listed in config.export.default
            resolve([dep.name for dep in mod.dependencies if not dep.optional or dep.name in config.export.default])
            if cmd not in resolved_cmds:
                resolved_cmds.append(cmd)

    resolve(invoked_cmds)

    entry_tasks: list[Callable[[Entry], Entry]] = []
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
        new_tasks = mod.export(config, module_data)

        # callables added to entry_tasks will be called for each entry
        entry_tasks.extend(new_tasks)

    # for each entry, do the needed tasks
    # TODO read_data actually loads the entire file, but here we should read one line at a time
    for entry in read_data(config)[2]:
        updated_entry = entry
        for task in entry_tasks:
            updated_entry = task(updated_entry)

from dataclasses import dataclass
import importlib

from karppipeline.common import PipelineException
from karppipeline.models import PipelineConfig


@dataclass
class Dependency:
    name: str
    optional: bool = False


def resolve_commands(config: PipelineConfig, subcommand: list[str] | None, defaults: list[str], cmd_type="export"):
    if not subcommand:
        init_invoked_cmds = defaults
    else:
        init_invoked_cmds = subcommand

    invoked_cmds = []
    for cmd in init_invoked_cmds:
        module_type = cmd
        if cmd in config.modules:
            cmd_config = config.modules[cmd]
            module_type = cmd_config.get("type") or cmd

        invoked_cmds.append((cmd, module_type))

    resolved_cmds = []
    mods = {}

    def resolve(invoked_cmds):
        """
        Traverses the dependency tree and adds dependencies to resolved_cmds in the order they need to run
        """
        for cmd, module_type in invoked_cmds:
            try:
                mod = importlib.import_module("karppipeline.modules." + module_type)
                mods[cmd] = mod
            except ModuleNotFoundError as e:
                raise PipelineException(f'Module "{module_type}" not found') from e
            # only add optional dependencies if they are listed in defaults
            resolve(
                [
                    (dep.name, dep.name)  # dependencies cannot have multiple instances
                    for dep in getattr(mod, f"{'install_' if cmd_type == 'install' else ''}dependencies", [])
                    if not dep.optional or dep.name in defaults
                ]
            )
            if cmd not in resolved_cmds:
                resolved_cmds.append(cmd)

    resolve(invoked_cmds)
    return resolved_cmds, mods

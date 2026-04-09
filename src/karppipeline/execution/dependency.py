from dataclasses import dataclass
import importlib

from karppipeline.common import PipelineException


@dataclass
class Dependency:
    name: str
    optional: bool = False


def resolve_commands(subcommand: list[str] | None, defaults: list[str], cmd_type="export"):
    if subcommand is None:
        invoked_cmds = defaults
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
                raise PipelineException(f"{cmd} not found") from e
            # only add optional dependencies if they are listed in defaults
            resolve(
                [
                    dep.name
                    for dep in getattr(mod, f"{'install_' if cmd_type == 'install' else ''}dependencies", [])
                    if not dep.optional or dep.name in defaults
                ]
            )
            if cmd not in resolved_cmds:
                resolved_cmds.append(cmd)

    resolve(invoked_cmds)
    return resolved_cmds, mods

import copy
from dataclasses import dataclass, field
import functools
import logging
import os
from pathlib import Path
from typing import Any, Iterator, cast
from karppipeline.common import ImportException, Map
from karppipeline.models import PipelineConfig
from karppipeline.util import yaml

logger = logging.getLogger(__name__)

__all__ = ["ConfigHandle", "load_config", "find_configs"]


@dataclass
class ConfigHandle:
    workdir: Path
    config_dict: Map
    parents: list[str] = field(default_factory=list)


def load_config(config_handle) -> PipelineConfig:
    config_dict = config_handle.config_dict
    config_dict["workdir"] = config_handle.workdir
    return PipelineConfig.model_validate(config_dict)


def find_configs() -> list[ConfigHandle]:
    return list(_find_configs())


def _find_configs() -> Iterator[ConfigHandle]:
    """
    Find all available resource configs in $CWD or below in the hierarchy.
    Resolves parent resources by looking for the parent-setting or looking
    one level above for another config.yaml (recursively). If a config.yaml
    has root: true, the recursion stops and the final parent is resolved.

    TODO:
    - use parent_config_paths for more than debugging, which will make sure
      that the paths printed by karp-pipeline print-config-tree will be the
      ones that are used. Collect the paths and merge based on those paths later.
    - when we find parent: <path> we do not check for parents for those resources
    """

    @functools.lru_cache
    def read_config(dir_path: Path) -> Map | None:
        config_path = dir_path / "config.yaml"
        if config_path.exists():
            with open(config_path) as fp:
                logger.info(f"Reading {config_path}")
                return yaml.load(fp)
        return None

    start_path = Path(os.getcwd())
    config = read_config(start_path)
    if not config:
        raise ImportException(f"config: could not a config in {start_path}")

    parent_configs = []
    # useful for debugging
    parent_config_paths: list[Path] = []
    path = start_path
    while config and not config.get("root", False):
        parent_config_paths.append(path)
        parent_configs.append(config)
        # recusively find all parents until there is no config.yaml OR it contains root: true OR it contains parent: <path>
        if config and "parent" in config:
            # if parent is set on config, just use it and stop iterating
            path = Path(cast(str, config["parent"]))
            # set config to parent_config
            config = read_config(Path(cast(str, path)).parent)
            if not config:
                raise ImportException(f"config: could not find parent ({path})")
            break
        path = path / ".."
        config = read_config(path)
    if config:
        parent_config_paths.append(path)
        parent_configs.append(config)
    parent_configs = list(reversed(parent_configs))
    # reverse parents to make it the correct order
    parent_config_paths = list(reversed(parent_config_paths))
    left = parent_configs[0]
    for right in reversed(parent_configs[1:]):
        left = _merge_configs(left, right)

    # now all parents of the current dir configs, current_dir_config can still be None
    current_dir_config = left

    def find_children(path: Path, parent: Map | None, parent_config_paths) -> list[tuple[dict[str, Any], list[Path]]]:
        children = []
        for dir in path.iterdir():
            if dir.is_dir():
                config = read_config(dir)
                if config:
                    if "parent" in config:
                        parent_path = Path(cast(str, config["parent"])).parent
                        if not parent_path.is_absolute():
                            parent_path = dir / parent_path
                        parent_config_paths = [parent_path]
                        # TODO parent_path does not resolve its own parents
                        other_parent = read_config(parent_path)
                        new_config = _merge_configs(other_parent, config)
                    else:
                        new_config = _merge_configs(parent, config)
                    new_children = find_children(dir, new_config, parent_config_paths + [dir])
                    if new_children:
                        children.extend(new_children)
                    else:
                        # insert workdir so we can find the correct place later
                        new_config["workdir"] = dir
                        children.append((new_config, parent_config_paths + [dir]))
        return children

    children = find_children(start_path, current_dir_config, parent_config_paths)

    if children:
        for child, parent_config_paths in children:
            yield ConfigHandle(
                workdir=child["workdir"],
                config_dict=child,
                parents=[str(path.absolute().resolve()) for path in parent_config_paths],
            )
    elif current_dir_config:
        yield ConfigHandle(
            workdir=start_path,
            config_dict=current_dir_config,
            parents=[str(path.absolute().resolve()) for path in parent_config_paths],
        )


def _merge_configs(orig_parent_config: Map | None, child_config: Map) -> Map:
    """
    Overwrites main_config with values from resource_config
    """
    if not orig_parent_config:
        return child_config
    parent_config = copy.deepcopy(orig_parent_config)
    for key, value in child_config.items():
        main_val = parent_config.get(key)
        if value is None:
            continue
        elif main_val and isinstance(main_val, dict) and isinstance(value, dict):
            tmp = _merge_configs(main_val, value)
            parent_config[key] = tmp
        else:
            parent_config[key] = value
    return parent_config

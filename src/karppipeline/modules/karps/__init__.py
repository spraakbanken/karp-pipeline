from karppipeline.execution.dependency import Dependency


"""
helper to run both karps_config and karps_data
"""

__all__ = ["export", "install", "dependencies"]

dependencies = [Dependency("karps_config"), Dependency("karps_data")]

install_dependencies = dependencies


def export(*_args, **_kwargs): ...


def install(*_args, **_kwargs): ...

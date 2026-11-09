import importlib.util
from pathlib import Path
from types import ModuleType


def import_module(path: Path) -> ModuleType | None:
    spec = importlib.util.spec_from_file_location("plugin", path)
    if not spec or not spec.loader:
        return None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

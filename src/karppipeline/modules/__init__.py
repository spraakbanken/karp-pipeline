import importlib
import pkgutil
from types import ModuleType
from typing import Iterable


def get_available_modules() -> Iterable[ModuleType]:
    modules: list[ModuleType] = []
    for module in pkgutil.iter_modules(path=__path__, prefix=f"{__package__}."):
        mod = importlib.import_module(module.name)
        modules.append(mod)
    return modules

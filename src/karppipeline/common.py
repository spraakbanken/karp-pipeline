from pathlib import Path

type Map = dict[str, object]


class PipelineException(Exception):
    pass


def create_output_dir(path: Path) -> Path:
    return _create_dir(path / "output")


def get_output_dir(path: Path) -> Path:
    return Path(path / "output")


def get_log_dir(path: Path) -> Path:
    return Path(path / "log")


def create_log_dir(path: Path) -> Path:
    return _create_dir(path / "log")


def _create_dir(path: Path) -> Path:
    path.mkdir(exist_ok=True)
    return path

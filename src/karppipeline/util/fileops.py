import logging
from pathlib import Path
import shlex
import shutil

from karppipeline.execution.install import DRY_RUN
from karppipeline.util import subprocess
from karppipeline.util.logging import dry_run_info


logger = logging.getLogger(__name__)


def copy(source, target, host=None) -> None:
    if host:
        dry_run_info(logger, f"Uploading output to host {host}, directory: {target}")
        if not DRY_RUN:
            subprocess.run_subprocess(
                ["rsync", str(source), f"{host}:{target}"], err_msg=f"Unable to copy file to host {host}"
            )
    else:
        dry_run_info(logger, f"Copying {source} to directory: {target}")
        if not DRY_RUN:
            shutil.copy(source, target)


def mkdir(dir: Path, host=None) -> None:
    if not host:
        if not DRY_RUN:
            dir.mkdir(exist_ok=True)
        else:
            dry_run_info(logger, f"Create directory {dir}")
    else:
        # subprocess handles DRY_RUN
        cmd = f'ssh {shlex.quote(host)} "mkdir -p {dir}"'
        subprocess.run_subprocess(
            cmd,
            shell=True,
            err_msg=f"Unable to create output directory on host {host}",
        )


def remove(files: list[Path]) -> None:
    for file_to_remove in files:
        if not DRY_RUN:
            file_to_remove.unlink(missing_ok=True)
        else:
            dry_run_info(logger, f"Remove {file_to_remove}")

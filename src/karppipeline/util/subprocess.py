import logging
import subprocess

from karppipeline.common import PipelineException
from karppipeline.execution.install import DRY_RUN
from karppipeline.util.logging import dry_run_info


logger = logging.getLogger(__name__)


def run_subprocess(cmd: str | list[str], err_msg=None, check=True, shell=False, print_output=True, cwd=None) -> int:
    log_output = f"Running subprocess: {''.join(cmd)}"
    if DRY_RUN:
        dry_run_info(logger, log_output)
        return 0
    else:
        logger.debug(log_output)
    kwargs = {}
    if cwd:
        kwargs["cwd"] = cwd
    if not DRY_RUN:
        p = subprocess.run(cmd, check=False, capture_output=True, shell=shell, encoding="utf-8", **kwargs)
        out = p.stdout
        err = p.stderr
        if print_output:
            if out:
                logger.debug(out)
            if err:
                logger.error(err)
        if check and p.returncode:
            raise PipelineException(err_msg)
        return p.returncode
    else:
        # if DRY_RUN is set, print the command to be run and return success error code
        cwd_logging = f", in working directory: '{cwd}'." if cwd else ""
        dry_run_info(logger, f"Run command '{''.join(cmd)}'{cwd_logging}")
        return 0

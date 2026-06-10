import logging
import subprocess

from karppipeline.common import PipelineException


logger = logging.getLogger(__name__)


def run_subprocess(cmd: str | list[str], err_msg=None, check=True, shell=False, print_output=True) -> int:
    logger.debug(f"Running subprocess: {cmd}")
    p = subprocess.run(cmd, check=False, capture_output=True, shell=shell, encoding="utf-8")
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

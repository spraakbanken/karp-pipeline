from pathlib import Path
import logging
import subprocess
from typing import Callable, Sequence

from pydantic import BaseModel

from karppipeline.common import InstallException, get_output_dir
from karppipeline.models import Entry, PipelineConfig
from karppipeline.run import Dependency

"""
generate SBX metadata file
"""

__all__ = ["export", "install", "dependencies"]


dependencies = [Dependency("jsonl")]

logger = logging.getLogger(__name__)


class DataUploadConfig(BaseModel):
    class Config:
        extra = "forbid"

    data_dir: Path
    remote_host: str | None = None


def export(_) -> Sequence[Callable[[Entry], Entry]]:
    return ()


def install(pipeline_config: PipelineConfig, uninstall=False):
    if uninstall:
        raise InstallException("Uninstall not supported for dataupload module")

    data_upload_config: DataUploadConfig = _get_config(pipeline_config)
    _upload_data(pipeline_config, data_upload_config)


def _get_config(pipeline_config: PipelineConfig):
    return DataUploadConfig.model_validate(pipeline_config.modules["dataupload"])


def _upload_data(pipeline_config: PipelineConfig, data_upload_config: DataUploadConfig):
    host = data_upload_config.remote_host
    data_dir = data_upload_config.data_dir
    output_dir = get_output_dir(pipeline_config.workdir)
    file = output_dir / f"{pipeline_config.resource_id}.jsonl"
    if host:
        logger.info(f"Uploading output to host {host}, directory: {data_dir}")
        subprocess.check_call(["rsync", str(file), f"{host}:{data_dir}"])
    else:
        logger.info(f"Copying output to directory: {data_dir}")
        subprocess.check_call(["cp", str(file), data_dir])

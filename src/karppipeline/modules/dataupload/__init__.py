from pathlib import Path
import logging

from pydantic import BaseModel

from karppipeline.common import PipelineException, get_output_dir
from karppipeline.execution.dependency import Dependency
from karppipeline.models import PipelineConfig
from karppipeline.util import fileops

"""
export: does nothing
install: copy jsonl output to another directory, optionally on a remote host
"""

__all__ = ["export", "install", "dependencies"]


dependencies = [Dependency("jsonl")]

logger = logging.getLogger(__name__)


class DataUploadConfig(BaseModel):
    class Config:
        extra = "forbid"

    data_dir: Path
    remote_host: str | None = None


def export(*_): ...


def install(pipeline_config: PipelineConfig, uninstall=False, instance="dataupload"):
    if uninstall:
        raise PipelineException("Uninstall not supported for dataupload module")

    data_upload_config: DataUploadConfig = _get_config(pipeline_config, instance)
    if pipeline_config.limited_access:
        logger.info("Cannot upload data for resource with limited access")
    else:
        _upload_data(pipeline_config, data_upload_config)


def _get_config(pipeline_config: PipelineConfig, instance):
    return DataUploadConfig.model_validate(pipeline_config.modules[instance])


def _upload_data(pipeline_config: PipelineConfig, data_upload_config: DataUploadConfig):
    host = data_upload_config.remote_host
    data_dir = data_upload_config.data_dir
    output_dir = get_output_dir(pipeline_config.workdir)
    file = output_dir / f"{pipeline_config.resource_id}.jsonl"
    fileops.copy(file, data_dir, host=host)

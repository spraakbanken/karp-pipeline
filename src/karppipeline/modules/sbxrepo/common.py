from pathlib import Path


from karppipeline.common import create_output_dir
from karppipeline.models import PipelineConfig
from karppipeline.modules.sbxrepo.models import SBXRepoConfig


def _get_config(pipeline_config: PipelineConfig, instance):
    return SBXRepoConfig.model_validate(pipeline_config.modules[instance])


def _get_metadata_file(pipeline_config: PipelineConfig) -> Path:
    output_dir = create_output_dir(pipeline_config.workdir) / "sbxrepo"
    output_dir.mkdir(exist_ok=True)
    return output_dir / _get_metadata_filename(pipeline_config.resource_id)


def _get_metadata_filename(resource_id: str) -> str:
    return f"{resource_id}.yaml"

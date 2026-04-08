from pathlib import Path
import shutil
import subprocess

from karppipeline.models import PipelineConfig
from karppipeline.modules.sbxrepo.models import SBXRepoConfig

from karppipeline.common import get_output_dir
from karppipeline.util.git import GitRepo
from karppipeline.modules.sbxrepo.common import _get_metadata_file


def _upload_data(pipeline_config: PipelineConfig, sbmetadata_config: SBXRepoConfig):
    host = sbmetadata_config.data.remote_host
    remote_dir = sbmetadata_config.data.data_dir
    output_dir = get_output_dir(pipeline_config.workdir)
    file = output_dir / f"{pipeline_config.resource_id}.jsonl"
    subprocess.check_call(["rsync", str(file), f"{host}:{remote_dir}"])


def _install_metadata_file(pipeline_config: PipelineConfig, sbmetadata_config: SBXRepoConfig):
    yaml_path = sbmetadata_config.metadata.yaml_export_path
    repo = GitRepo(yaml_path)

    resource_id = pipeline_config.resource_id
    metadata_yaml = _get_metadata_file(pipeline_config)

    main_dir = Path(yaml_path)
    # TODO versioning may affect name of file
    shutil.copy(metadata_yaml, main_dir / "yaml/lexicon" / f"{resource_id}.yaml")
    repo.commit_all(msg=f"add {pipeline_config.resource_id}", allow_empty=False)

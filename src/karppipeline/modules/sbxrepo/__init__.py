from typing import Any, Callable, Sequence


from karppipeline.common import InstallException
from karppipeline.models import Entry, PipelineConfig
from karppipeline.run import Dependency

"""
exporter generates SBX metadata file
installer moves file to the configured Git repo and commits
"""

__all__ = ["export", "install", "dependencies"]


dependencies = [Dependency("sbxmetadata", optional=True), Dependency("schema"), Dependency("dataupload")]


def export(config: PipelineConfig, module_data: dict[str, Any]) -> Sequence[Callable[[Entry], Entry]]:
    """
    This module creates a metadata file valid for the SBX repo (https://spraakbanken.gu.se/om/internt/teknik/metadata).

    It depends on the module sbxmetadata (metadata API).
    """
    from karppipeline.modules.sbxrepo.metadata import _create_sb_metadata_file

    metadata = module_data["sbxmetadata"] or {}
    schema_data = module_data["schema"]

    # create and validate file, save it in output directory
    _create_sb_metadata_file(config, schema_data["size"], metadata)
    return ()


def install(pipeline_config: PipelineConfig, uninstall=False):
    if uninstall:
        raise InstallException("Uninstall not supported for sbxrepo module")
    from karppipeline.modules.sbxrepo.common import _get_config
    from karppipeline.modules.sbxrepo.installer import _install_metadata_file
    from karppipeline.modules.sbxrepo.models import SBXRepoConfig

    sbmetadata_config: SBXRepoConfig = _get_config(pipeline_config)
    _install_metadata_file(pipeline_config, sbmetadata_config)

import logging
from typing import Any, Final


from karppipeline.common import PipelineException
from karppipeline.execution.dependency import Dependency
from karppipeline.models import PipelineConfig

"""
exporter generates SBX metadata file
installer moves file to the configured Git repo and commits
"""

__all__ = ["export", "install", "dependencies"]


dependencies = [Dependency("sbxmetadata", optional=True), Dependency("schema")]
install_dependencies = [Dependency("dataupload")]

logger = logging.getLogger(__name__)

MODULE_NAME: Final[str] = "sbxrepo"


def export(config: PipelineConfig, module_data: dict[str, Any], instance=MODULE_NAME):
    """
    This module creates a metadata file valid for the SBX repo (https://spraakbanken.gu.se/om/internt/teknik/metadata).

    It depends on the module sbxmetadata (metadata API).
    """
    from karppipeline.modules.sbxrepo.metadata import _create_sb_metadata_file

    metadata = module_data["sbxmetadata"] or {}
    schema_data = module_data["schema"]

    # create and validate file, save it in output directory
    _create_sb_metadata_file(config, schema_data["size"], metadata, instance)


def install(pipeline_config: PipelineConfig, uninstall=False, instance=MODULE_NAME):
    if uninstall:
        raise PipelineException("Uninstall not supported for sbxrepo module")

    if pipeline_config.protected_metadata:
        logger.info("Cannot add metadata file to repo when metadata is protected")
        return

    from karppipeline.modules.sbxrepo.common import _get_config
    from karppipeline.modules.sbxrepo.installer import _install_metadata_file
    from karppipeline.modules.sbxrepo.models import SBXRepoConfig

    sbmetadata_config: SBXRepoConfig = _get_config(pipeline_config, instance)
    _install_metadata_file(pipeline_config, sbmetadata_config)
    logger.info(f"Added SBX metadata file to {sbmetadata_config.metadata.yaml_export_path}")

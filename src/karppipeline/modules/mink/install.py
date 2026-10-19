import logging
from pathlib import Path

from karppipeline.common import PipelineException, create_output_dir, get_output_dir
from karppipeline.models import NonEmptyMultiLang, PipelineConfig
from karppipeline.modules.karps_config import MODULE_NAME as KARPS_MODULE_NAME
from karppipeline.modules.karps.models import KarpsExportConfig
from karppipeline.modules.mink.mink_api import MinkAPI
from karppipeline.modules.mink.models import InstallOption, MinkConfig
from karppipeline.util import yaml
from karppipeline.util import json

logger = logging.getLogger(__name__)


def mink_create(config: PipelineConfig, mink_config: MinkConfig, mink_api: MinkAPI) -> tuple[bool, str]:
    resource_id = _get_mink_resource_id(config) or config.resource_id
    # call API to create Mink resource
    actual_resource_id = mink_api.create_lexicon(resource_id)

    if actual_resource_id != resource_id:
        logger.info(
            f"Created a new Mink resource with id: {actual_resource_id}. Future pipeline calls will work with this ID until karp-pipeline clean is used."
            " Update config.yaml to make the pipeline use this ID always."
        )
        _save_mink_resource_id(config, actual_resource_id)
    else:
        logger.info(f"Using preconfigured Mink resource with id: {actual_resource_id}")

    # generate mink karp-pipeline config
    karps_config = config.modules[KARPS_MODULE_NAME]
    if mink_config.karps:
        karps_config = mink_config.karps
    elif not karps_config:
        raise PipelineException("Either configure mink.karps or karps module to make Mink installation work.")
    if "link" not in karps_config:
        karps_config["link"] = f"https://spraakbanken.gu.se/mink/library/lexicon/{actual_resource_id}"
    # validate and instantiate
    karps_config_model_instance = KarpsExportConfig.model_validate(karps_config)

    name = config.name if config.name else NonEmptyMultiLang(actual_resource_id)
    description = config.description if config.description else NonEmptyMultiLang(actual_resource_id)

    # can't use the PipelineConfig class since parent is not a part of model and fields are missing (inherited from parent)
    config_dict = {
        "resource_id": actual_resource_id,
        "name": name.model_dump(),
        "description": description.model_dump(),
        "karps": karps_config_model_instance.model_dump(exclude_defaults=True, exclude_unset=True),
        "karp_red": {"replace": True},
        "parent": "/home/fksb/karp-config/config.yaml",
    }

    # Dump it for inspection purposes
    with open(_get_config_file_path(config), "wb") as fp:
        json.dump(config_dict, fp)

    # here we must use config.resource_id and not the Mink resource_id (though they might be the same)
    data_path = get_output_dir(config.workdir) / f"{config.resource_id}.jsonl"
    mink_api.upload_data(data_path)
    logger.info(f"Uploaded {data_path}")

    mink_config_text = yaml.dumps(config_dict)

    mink_api.upload_config(mink_config_text)

    logger.info("Running Karp pipeline via Mink.")
    return mink_api.run_pipeline(), actual_resource_id


def mink_install(resource_id: str, mink_config: MinkConfig, mink_api: MinkAPI, target: InstallOption):
    if target not in mink_config.install:
        return

    logger.info(f"Installing into {target.pretty_str()} mode via Mink.")
    install_success = mink_api.install_karp_system(target)
    if install_success:
        if target == InstallOption.KARP_S:
            url = f"https://spraakbanken.gu.se/karp/?resources={resource_id}"
        else:
            url = f"https://spraakbanken4.it.gu.se/karp/v8-dev_1/query/{resource_id}"

        logger.info(f"Resource should be available at {url}")


def mink_uninstall(config: PipelineConfig, mink_api: MinkAPI) -> None:
    resource_id = _get_mink_resource_id(config) or config.resource_id
    # initalize API object without trying to create resource
    mink_api.init(resource_id)
    # also uninstalls in Karp-s
    success = mink_api.delete_lexicon()
    if success:
        logger.info(f"Resource deleted: {config.resource_id}")
    else:
        logger.info(f"Resource not deleted: {config.resource_id}")


def _get_config_file_path(config) -> Path:
    """
    The config uploaded to Mink is saved in this file.
    """
    path = _get_output_dir(config)
    return path / "config.json"


def _get_resource_id_file_path(config) -> Path:
    """
    The resource ID given by Mink is saved in this file.
    """
    path = _get_output_dir(config)
    return path / "mink_resource_id"


def _get_mink_resource_id(config) -> str | None:
    """
    Get resource ID given by Mink in previous runs
    """
    path = _get_resource_id_file_path(config)
    if path.exists():
        with open(path) as fp:
            return fp.read()
    return None


def _save_mink_resource_id(config, resource_id) -> None:
    """
    Write resource ID give by Mink.
    """
    path = _get_resource_id_file_path(config)
    with open(path, "w") as fp:
        fp.write(resource_id)


def _get_output_dir(config) -> Path:
    path = create_output_dir(config.workdir) / "mink"
    path.mkdir(exist_ok=True)
    return path

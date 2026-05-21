from datetime import datetime
from typing import cast
import urllib.request
from karppipeline.common import PipelineException
from karppipeline.util.frozendict import frozendict

import jsonschema_rs


from karppipeline.models import PipelineConfig
from karppipeline.modules.sbxrepo.models import SBXRepoConfig
from karppipeline.util import json, yaml
from karppipeline.modules.sbxrepo.common import _get_config, _get_metadata_file


def _create_sb_metadata_file(pipeline_config: PipelineConfig, size, metadata: dict[str, object], instance: str):
    sbxmetadata_config: SBXRepoConfig = _get_config(pipeline_config, instance)

    metadata["size"] = {"entries": size}

    configured_metadata = sbxmetadata_config.metadata.model_dump(
        exclude_none=True, exclude={"yaml_export_path", "schema_", "license", "fallbacks"}, exclude_unset=True
    )

    # add configured values to target
    metadata.update(configured_metadata)

    if "name" not in metadata:
        if not pipeline_config.name:
            raise PipelineException("sbxrepo: 'name' not found")
        metadata["name"] = pipeline_config.name.model_dump(exclude_none=True)
    if "short_description" not in metadata and "description" not in metadata:
        if not pipeline_config.description:
            raise RuntimeError("sbxrepo: 'description' not found")
        # metadata repo also supports short_description, but we use HTML in our description and it is not allowed in metadata
        metadata["description"] = pipeline_config.description.model_dump(exclude_none=True)

    date_str = _get_current_date_string()
    if "created" not in metadata:
        metadata["created"] = date_str
    if not sbxmetadata_config.metadata.updated:
        metadata["updated"] = date_str

    # using immutable dicts to make it possible to use sets
    if "downloads" not in metadata:
        downloads = []
    else:

        def ensure_download_type(elem):
            # if type=lexicon was omitted before, it will be replaced by an identical download with type set
            if elem.get("format") == "jsonl":
                elem["type"] = "lexicon"
            return elem

        downloads = [frozendict(ensure_download_type(elem)) for elem in cast(list, metadata["downloads"])]
    for download in sbxmetadata_config.metadata.downloads or ():
        frozen_download = frozendict(download)
        if frozen_download not in downloads:
            downloads.append(frozen_download)

    if not pipeline_config.limited_access:
        # only add download if the resource is open
        frozen_download = frozendict(
            {
                "url": sbxmetadata_config.data.download_url_template.format(resource_id=pipeline_config.resource_id),
                "license": sbxmetadata_config.metadata.license,
                "format": "jsonl",
                "type": "lexicon",
            }
        )
        downloads.append(frozen_download)
    # unfreeze
    metadata["downloads"] = [dict(download) for download in downloads]

    if not sbxmetadata_config.metadata.interfaces:
        metadata["interfaces"] = [
            {
                "url": sbxmetadata_config.data.interface_url_template.format(resource_id=pipeline_config.resource_id),
                "license": sbxmetadata_config.metadata.license,
            }
        ]
    metadata["type"] = "lexicon"

    if "contact_info" not in metadata:
        if not sbxmetadata_config.metadata.fallbacks:
            raise RuntimeError("sbxrepo: 'contact_info' not found")
        metadata["contact_info"] = sbxmetadata_config.metadata.fallbacks.contact_info

    # load and test against JSON schema for SBX metadata
    with urllib.request.urlopen(sbxmetadata_config.metadata.schema_) as response:
        content = response.read().decode("utf-8")
        schema = json.loads(content)
    try:
        jsonschema_rs.validate(schema, metadata)
    except jsonschema_rs.ValidationError as exc:
        raise ImportError("metadata file not valid") from exc

    with open(_get_metadata_file(pipeline_config), "w") as fp:
        yaml.dump(metadata, fp)


def _get_current_date_string():
    return datetime.now().strftime("%Y-%m-%d")

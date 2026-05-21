from pathlib import Path
import urllib.request
from json import JSONDecodeError
from urllib.error import HTTPError, URLError
from karppipeline.common import create_output_dir
import karppipeline.util.json as json

from karppipeline.models import PipelineConfig

__all__ = ["export", "load", "dependencies"]


dependencies = []


def export(config: PipelineConfig, _, **_kwargs):
    """
    Fetches available metadata from SBX metadata API.
    """
    metadata = _fetch_metadata_from_api(config.resource_id)
    with open(_get_data_path(config), "w") as fp:
        fp.write(json.dumps(metadata))


def load(config: PipelineConfig) -> dict[str, object]:
    with open(_get_data_path(config)) as fp:
        metadata = json.loads(fp.read())
    return metadata


def _get_data_path(config: PipelineConfig) -> Path:
    module_dir = create_output_dir(config.workdir) / "sbxmetadata"
    module_dir.mkdir(exist_ok=True)
    return module_dir / "metadata.json"


def _fetch_metadata_from_api(resource_id) -> dict[str, object]:
    url = f"https://ws.spraakbanken.gu.se/ws/metadata/v3/?resource={resource_id}&legacy=false"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode("utf-8")
    except HTTPError as e:
        raise RuntimeError(f"Error when calling metadata API on {url}") from e
    except URLError as e:
        raise RuntimeError(f"Metadata API not reachable on {url}") from e
    try:
        metadata = json.loads(body)
        if metadata:
            # these two properties are not valid in a metadata *file*
            del metadata["has_description"]
            del metadata["id"]

            # removing auto set values
            for download in metadata.get("downloads", ()):
                del download["last-modified"]
                del download["size"]

            # only language code should be used in the metadata file
            metadata["language_codes"] = []
            for language in metadata["languages"]:
                metadata["language_codes"].append(language["code"])
            del metadata["languages"]
            for download in metadata.get("downloads", []):
                license = download["license"]["id"]
                download["license"] = license

        return metadata
    except JSONDecodeError:
        return {}

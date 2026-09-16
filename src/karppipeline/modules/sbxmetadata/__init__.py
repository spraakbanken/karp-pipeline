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
    url = f"https://ws.spraakbanken.gu.se/ws/metadata/v4/source/lexicon/{resource_id}"
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
        return metadata
    except JSONDecodeError:
        return {}

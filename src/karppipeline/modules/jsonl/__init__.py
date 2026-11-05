import logging
from karppipeline.execution.dependency import Dependency
from karppipeline.models import Entry, PipelineConfig

from karppipeline.common import create_output_dir
from karppipeline.util import json

__all__ = ["export", "dependencies"]
logger = logging.getLogger(__name__)

dependencies = [Dependency("schema")]


def export(config: PipelineConfig, _, **_kwargs):
    """
    Writes each entry to file
    """

    fp = open(create_output_dir(config.workdir) / f"{config.resource_id}.jsonl", "w")

    def task(entry: Entry | None, /) -> Entry | None:
        if not entry:
            fp.close()
            return None
        logger.debug("jsonl entry task")
        fp.write(json.dumps(entry) + "\n")
        return entry

    return task

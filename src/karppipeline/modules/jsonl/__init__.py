import logging
from karppipeline.models import Entry, PipelineConfig

from karppipeline.common import create_output_dir
from karppipeline.run import Dependency
from karppipeline.util import json

__all__ = ["export", "dependencies"]
logger = logging.getLogger(__name__)

dependencies = [Dependency("schema")]


def export(config: PipelineConfig, _):
    """
    Writes each entry to file
    """

    def json_dump():
        with open(create_output_dir(config.workdir) / f"{config.resource_id}.jsonl", "w") as fp:
            while True:
                entry = yield
                if not entry:
                    break
                fp.write(json.dumps(entry) + "\n")

    gen = json_dump()
    next(gen)

    def task(entry: Entry, /) -> Entry:
        logger.debug("jsonl entry task")
        gen.send(entry)
        return entry

    return (task,)

import logging
from pathlib import Path
import pickle
from karppipeline.common import create_output_dir
from karppipeline.modules.schema.entry_task import get_entry_converter
from karppipeline.modules.schema.schema_creator import pre_import_resource
from karppipeline.util import json

logger = logging.getLogger(__name__)

__all__ = ["export", "dependencies", "load"]


# generate schema, source_order and size
dependencies = []


def export(config, _):
    """
    Loads the input data and deduces schema, source order and size.

    Returns the task for doing all field conversions.
    """
    # pre-import tasks, invoke conversions to know which fields *will* be there
    entry_schema, source_order, [size] = pre_import_resource(config)

    # modifies entry_schema based on config and returns modification task for entries
    entry_converter = get_entry_converter(config, entry_schema)

    logger.info("Using entry schema: " + json.dumps(entry_schema))

    with open(_get_data_path(config), "wb") as fp:
        pickle.dump({"entry_schema": entry_schema, "source_order": source_order, "size": size}, fp)

    # return task to include, exclude, rename or update fields in enries (based on export.fields)
    return (entry_converter,)


def load(config) -> dict[str, object]:
    with open(_get_data_path(config), "rb") as fp:
        return pickle.load(fp)


def _get_data_path(config) -> Path:
    module_dir = create_output_dir(config.workdir) / "schema"
    module_dir.mkdir(exist_ok=True)
    return module_dir / "schema.pickle"

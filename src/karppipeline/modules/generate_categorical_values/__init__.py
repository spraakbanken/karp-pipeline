from collections import defaultdict
import logging
from pathlib import Path
from karppipeline.common import create_output_dir
from karppipeline.execution.dependency import Dependency
from karppipeline.models import Entry, EntrySchema, PipelineConfig
from karppipeline.util import yaml

__all__ = ["export", "dependencies"]
logger = logging.getLogger(__name__)

dependencies = [Dependency("schema")]


def export(config: PipelineConfig, module_data):
    entry_schema: EntrySchema = module_data["schema"]["entry_schema"]

    fields = []
    for field in entry_schema.values():
        # if field is configured as categorical, but categories are not set
        if field.categories is not None and not field.categories:
            fields.append(field.name)

    def category_collector():
        field_categories = defaultdict(set)
        while True:
            entry = yield

            if not entry:
                break

            for field_name in fields:
                if field_name in entry:
                    field_categories[field_name].add(entry[field_name])

        # create module output dir
        main_output_dir = create_output_dir(config.workdir)
        p = Path("generate_categorical_values")
        module_dir = main_output_dir / p
        module_dir.mkdir(exist_ok=True)

        for field, categories in field_categories.items():
            filename = module_dir / f"{field}.yaml"
            with open(filename, "w") as fp:
                labels = {category: {"swe": category, "eng": category} for category in sorted(categories)}
                fp.write(yaml.dumps({"labels": labels}))
                fp.write(yaml.dumps({"categories": list(categories)}))
            logger.info(f"Auto-created categorical values for {field} in {filename}")

    gen = category_collector()
    next(gen)

    def entry_task(entry: Entry) -> Entry:
        try:
            gen.send(entry)
        except StopIteration:
            ...
        return entry

    return [entry_task]

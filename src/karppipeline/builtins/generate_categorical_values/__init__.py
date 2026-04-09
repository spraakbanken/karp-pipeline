from karppipeline.models import PipelineConfig
from karppipeline.read import read_data
from karppipeline.util import yaml


def generate_categorical_values(config: PipelineConfig, field_name):
    _, _, entries = read_data(config)
    categories = set()
    for entry in entries:
        if field_name in entry:
            categories.add(entry[field_name])
    if not categories:
        print("not found")
    else:
        categories = sorted(categories)
        labels = {category: {"swe": category, "eng": category} for category in categories}
        print(yaml.dumps({"labels": labels}))
        print(yaml.dumps({"categories": categories}))

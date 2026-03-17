"""
Used to insert source links in resource so2009-ord
"""
import importlib.resources
from karppipeline.models import InferredField
from karppipeline.util import json

HOST = "https://spraakbanken.gu.se/resurser/data/karp/so-2009-webbversion"
TEMPLATE = HOST + "/{word}.html"


def create_link_update_schema(field) -> InferredField:
    return InferredField(name=field.name, type="text", extra={"length": 101})


data = importlib.resources.read_binary("karppipeline.converters.sosource", "available_words.json")
available_words = json.load_array(data)


def create_link(resource_id: str, entry) -> str | None:
    word = entry["normaliserat_ord"]
    if word in available_words:
        return TEMPLATE.format(word=entry["normaliserat_ord"])
    elif entry.get("stycke") in available_words:
        return TEMPLATE.format(word=entry["stycke"])
    else:
        return None

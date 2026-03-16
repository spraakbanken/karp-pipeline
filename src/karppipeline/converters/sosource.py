from karppipeline.models import InferredField

HOST = "https://spraakbanken.gu.se/resurser/data/karp/so-2009-webbversion"
TEMPLATE = HOST + "/{word}.html"

def create_link_update_schema(field) -> InferredField:
    return InferredField(name=field.name, type="text", extra={"length": 101})


def create_link(resource_id: str, entry) -> str:
    return TEMPLATE .format(word=entry["ursprungligt_ord"])

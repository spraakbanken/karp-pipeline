from karppipeline.models import EntrySchema


def add_namespace_to_schema(namespace: str, schema: EntrySchema) -> EntrySchema:
    new_schema = {}
    for key, val in schema.items():
        new_key = add_namespace_to_field(namespace, key)
        new_field = val.copy()
        new_field.name = new_key
        new_schema[new_key] = new_field
    return new_schema


def add_namespace_to_field(namespace: str, field: str) -> str:
    return "_" + namespace + "_" + field

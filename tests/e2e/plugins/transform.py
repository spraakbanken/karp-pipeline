from karppipeline.models import Entry, EntrySchema
from typing import Iterable


def update_schema(entry_schema: EntrySchema):
    copy = entry_schema["lemgram"].copy()
    copy.collection = False
    entry_schema["lemgram"] = copy
    return entry_schema


def update_source_order(source_order: list[tuple[str, list]]) -> list[str]:
    elems = []
    for elem in source_order:
        elems.append(elem[0])
    return elems


def transform(entry: Entry) -> Iterable[Entry]:
    for lemgram in entry.get("lemgram", ()):
        if "lemgram" in entry:
            del entry["lemgram"]  # pyright: ignore[reportIndexIssue]
        new_entry = {
            "lemgram": lemgram,
        }
        new_entry.update(entry)
        yield new_entry

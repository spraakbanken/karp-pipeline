import csv
import logging
from pathlib import Path
from typing import Iterator, cast

from karppipeline.common import ImportException
from karppipeline.models import Entry, PipelineConfig
from karppipeline.util import json

logger = logging.getLogger(__name__)


def _update_json_source_order(source_order: list[str], new_keys: list[str]) -> list[str]:
    """
    Tries to merge two lists so that the order of original list is preserved, while new
    elements are added in between in appropriate places. If the order is conflicting
    we don't really care what happens, order should be hard coded in cofig for those cases.
    """
    source_place = 0
    for i, key in enumerate(new_keys):
        if key in source_order:
            source_place = source_order.index(key)
            continue

        # find anchor - find the next elment in keys that are already in
        source_order_from_current = source_order[source_place:]
        for future_key in new_keys[i:]:
            if future_key in source_order_from_current:
                # but get the index  from source_order
                anchor_idx = source_order.index(future_key)
                # splice in the new element immediately before anchor
                source_order.insert(anchor_idx, key)
                source_place = anchor_idx
                break
        else:
            # anchor not found - add
            source_order.append(key)
    return source_order


def _find_source_files(pipeline_config: PipelineConfig) -> tuple[list[Path], str]:
    files = list(pipeline_config.workdir.glob("source/*"))
    # TODO check that all files have the same file ending
    logger.info(f"Reading source files: {', '.join([str(file) for file in files])}")
    suffix = files[0].suffix
    return files, suffix


def read_data(pipeline_config: PipelineConfig) -> tuple[list[str], list[int], Iterator[Entry]]:
    """
    When reading CSV data, we know the fields and their order beforehand, but not for JSON
    (unless hard coded in configuration). We prepare source order here, but it is not usable
    until after the generators have been consumed, same as size.
    """
    input_files, suffix = _find_source_files(pipeline_config)

    # size, array because generator needs mutable object
    size = [0]
    source_order: list[str] = []
    if suffix in [".csv", ".tsv"]:

        def get_entries() -> Iterator[Entry]:
            for input_file in input_files:
                fp = open((input_file), encoding="utf-8-sig")
                if suffix == ".csv":
                    reader = csv.reader(fp)
                else:
                    reader = csv.reader(fp, dialect="excel-tab")

                file_source_order = next(reader, None) or []
                if not source_order:
                    for elem in file_source_order:
                        source_order.append(elem)
                else:
                    if source_order != file_source_order:
                        raise RuntimeError("Differing headers in CSV/TSV files")
                import_settings = cast(dict[str, dict[str, list[dict[str, str]]]], pipeline_config.import_settings)
                # type information for parsing values
                cast_fields: list[dict[str, str]] = import_settings["csv"]["cast_fields"]

                for row in reader:
                    entry: dict[str, str | int | float] = dict(zip(source_order, row))
                    # parse values
                    for field in cast_fields:
                        if field["type"] == "int":
                            entry[field["name"]] = int(entry[field["name"]])
                        elif field["type"] == "float":
                            entry[field["name"]] = float(entry[field["name"]])
                        else:
                            raise RuntimeError(f"Uknown type: {field['type']}, given in CSV import")
                    size[0] += 1
                    yield entry
                fp.close()

    else:

        def get_entries() -> Iterator[Entry]:
            for input_file in input_files:
                with open(input_file) as fp:
                    try:
                        for line in fp:
                            entry = json.loads(line)

                            # get the sort order from the input JSON
                            # this could be configurable to speed up
                            keys = list(entry.keys())
                            _update_json_source_order(source_order, keys)
                            size[0] += 1
                            yield entry
                    except UnicodeDecodeError:
                        raise ImportException(f"Unicode decode error for file: {input_file}")

    return source_order, size, get_entries()

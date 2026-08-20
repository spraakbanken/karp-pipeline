import csv
from json import JSONDecodeError
import logging
from pathlib import Path
from typing import Iterator, cast

from karppipeline.common import PipelineException
from karppipeline.execution.dependency import load_importer
from karppipeline.models import Entry, PipelineConfig
from karppipeline.util import json

logger = logging.getLogger(__name__)


def _update_json_source_order(source_order: list[tuple[str, list]], entry: Entry) -> None:
    # maybe source order should be an ordered dict here

    """
    Tries to merge the structure in source order with the structure of entry
    # so that the order of source_order preserved, while new elements are added in between in
    # appropriate places. If the order is conflicting we don't really care what happens,
    # order should be hard coded in config for those cases.
    """
    new_keys = []
    for key in entry.keys():
        for name, inner in source_order:
            if name == key:
                inner_source_order = inner
                break
        else:
            inner_source_order = []

        if isinstance(entry[key], list):
            for elem in entry[key]:
                if isinstance(elem, dict):
                    _update_json_source_order(inner_source_order, elem)
        if isinstance(entry[key], dict):
            _update_json_source_order(inner_source_order, cast(dict, entry[key]))
        new_keys.append((key, inner_source_order))

    _merge_new_sort_order(source_order, new_keys)


def _merge_new_sort_order(source_order_in: list[tuple[str, list]], new_keys_in: list[tuple[str, list]]) -> None:
    """
    Tries to merge the structure in source order with the structure of entry
    so that the order of source_order preserved, while new elements are added in between in
    appropriate places. If the order is conflicting we don't really care what happens,
    order should be hard coded in config for those cases.

    ignore the inner lists as they are fixed by caller

    does this in place, since references of source_orders are given to modules before all
    entries are read.
    """

    source_place = 0
    for i, (key, inner) in enumerate(new_keys_in):
        new_keys = [key for key, _ in new_keys_in]
        source_order = [key for key, _ in source_order_in]

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
                source_order_in.insert(anchor_idx, (key, inner))
                source_place = anchor_idx
                break
        else:
            # anchor not found - add
            source_order_in.append((key, inner))


def _find_source_files(pipeline_config: PipelineConfig) -> tuple[list[Path], str]:
    files = list(pipeline_config.workdir.glob("source/*"))
    # TODO check that all files have the same file ending
    logger.info(f"Reading source files: {', '.join([str(file) for file in files])}")
    suffix = files[0].suffix
    return files, suffix


def read_data(pipeline_config: PipelineConfig) -> tuple[list[str], list[int], Iterator[Entry]]:
    source_type = pipeline_config.import_settings.get("source_type", None)
    if source_type:
        instance_name, mod = load_importer(pipeline_config, cast(str, source_type))
        input_files, suffix = mod(pipeline_config, instance=instance_name)
    else:
        input_files, suffix = _find_source_files(pipeline_config)

    # When reading CSV data, we know the fields and their order beforehand, but not for JSON
    # (unless hard coded in configuration). We prepare source order here, but it is not usable
    # until after the generators have been consumed, same as size.

    # size, array because generator needs mutable object
    size = [0]
    # recursive since we now support trees
    source_order: list[tuple[str, list]] = []
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
                        source_order.append((elem, []))
                else:
                    if source_order != file_source_order:
                        raise RuntimeError("Differing headers in CSV/TSV files")
                import_settings = cast(dict[str, dict[str, list[dict[str, str]]]], pipeline_config.import_settings)
                # type information for parsing values
                cast_fields: list[dict[str, str]] = import_settings["csv"]["cast_fields"]

                for row in reader:
                    entry: dict[str, str | int | float] = dict(zip([elem[0] for elem in source_order], row))
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

        def get_entries_from_file() -> Iterator[Entry]:
            """
            Decides if we should parse the whole while as JSON or each row as JSON (JSONL)
            Yields entries
            """
            # must be a top-level array for now
            for input_file in input_files:
                with open(input_file, "rb") as fp:
                    try:
                        if suffix == ".json":
                            # if json - parse as array and yield all elements in file
                            try:
                                elems: list[Entry] = json.load_array(fp.read())
                            except JSONDecodeError:
                                raise PipelineException(f"Could not parse JSON in: {input_file}")
                            yield from elems
                        else:
                            # if jsonl - read one line at a time, parse and yield
                            for line_nr, line in enumerate(fp):
                                if not line.strip():
                                    # we allow empty lines
                                    continue
                                try:
                                    entry = json.loads(line)
                                except JSONDecodeError:
                                    raise PipelineException(f"Could not parse JSON on line: {line_nr}")
                                yield entry

                    except UnicodeDecodeError:
                        raise PipelineException(f"Unicode decode error for file: {input_file}")

        def get_entries() -> Iterator[Entry]:
            entries = get_entries_from_file()
            # get the sort order from the input JSON
            # this could be configurable to speed up
            for entry in entries:
                _update_json_source_order(source_order, entry)
                size[0] += 1
                yield entry

    return source_order, size, get_entries()

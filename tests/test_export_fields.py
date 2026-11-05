import pytest

from pathlib import Path
from karppipeline.common import PipelineException
from karppipeline.models import ExportConfig, ExportFieldConfig, PipelineConfig
from karppipeline.modules.schema.entry_task import get_entry_converter
from karppipeline.modules.schema.schema_creator import _create_fields


config = PipelineConfig(resource_id="test", export=ExportConfig(), workdir=Path("."), plugin_files=[])


def _get_converter(export, entry):
    schema = _create_fields(config, iter([entry]))
    return get_entry_converter(config, export, schema)


def test_field_conversion():
    entry = {"wordforms": ["tester", "testet"], "baseform": "test", "pos": "NN", "hierarchical": {"test": ["data"]}}

    field_export: list[ExportFieldConfig] = [
        ExportFieldConfig("not baseform"),
        ExportFieldConfig("pos:ud.suc_to_ud as upos"),
        ExportFieldConfig("..."),
    ]

    converter = _get_converter(field_export, entry)

    new_entry = converter(entry)
    assert new_entry == {
        "wordforms": ["tester", "testet"],
        "pos": "NN",
        "hierarchical": {"test": ["data"]},
        "upos": "NOUN",
    }


def test_missing_name():
    with pytest.raises(PipelineException):
        field_export = [ExportFieldConfig("pos:ud.suc_to_ud")]
        _get_converter(field_export, {"pos": "NN"})


def test_no_include_all():
    field_export = [ExportFieldConfig("pos")]
    entry = {"pos": "NN"}
    c = _get_converter(field_export, entry)
    assert entry == c(entry)

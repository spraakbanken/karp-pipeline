from dataclasses import dataclass
from pathlib import Path
import pickle
from typing import Final, Sequence
from pydantic import BaseModel

from karppipeline.common import create_output_dir, get_output_dir
from karppipeline.models import ExportFieldConfig, MultiLang, PipelineConfig


table_metadata_file: Final[str] = "tablemetadata.pickle"


@dataclass
class TableMetadata:
    csv_file: Path
    table: str
    columns: Sequence[str]

    @property
    def columns_str(self):
        return f"({', '.join((['`' + column + '`' for column in self.columns]))})"


class Tag(BaseModel):
    label: MultiLang
    description: MultiLang


class EntryWord(BaseModel):
    # description of the entry word field
    field: str
    description: MultiLang


class KarpsExportConfig(BaseModel):
    # which charset to use in MariaDB
    db_charset: str = "utf8mb4"
    # which MariaDB collation to use
    db_collation: str = "utf8mb4_swedish_ci"
    # which field to use as entry_word
    entry_word: EntryWord
    # tags that this resource belong to
    tags: list[str] = []
    # descrption of tags, probably set this in a parent config.yaml
    tags_description: dict[str, Tag] = {}
    # a link for this resource, maybe a home page or repository
    link: str
    # give either primary or secondary, depending on which list is easiest to populate. the other list will be populated automatically
    primary: list[str] = []
    secondary: list[str] = []
    export_fields: list[ExportFieldConfig] = []


class KarpsInstallConfig(KarpsExportConfig):
    # the directory where pipeline should put new resources
    output_config_dir: Path
    cli_path: Path
    # this both decides where to put files and where the cli is
    config_host: str | None = None
    db_database: str
    db_host: str | None = None


def get_export_config(config, instance):
    if instance in ["karps_config", "karps_data"]:
        # this module only uses the karps config and does not have it's own config
        instance = "karps"

    # TODO what if run karps-21 is run, how will the submodules know which instance to use??
    # TODO what if run karps_config, how will the submodule know which instance to use??
    # probably the modules need to be linked more and devise a way to use the submodules with instance settings

    # run karps_config

    module_config = KarpsExportConfig.model_validate(config.modules[instance])
    return module_config


def get_install_config(config, instance):
    if instance in ["karps_config", "karps_data"]:
        # this module only uses the karps config and does not have it's own config
        instance = "karps"

    return KarpsInstallConfig.model_validate(config.modules[instance])


def save_table_metadata(config: PipelineConfig, tables_metadata: list[TableMetadata]):
    path = create_output_dir(config.workdir) / "karps" / table_metadata_file
    with open(path, "wb") as fp:
        pickle.dump(tables_metadata, fp)


def get_table_metadata(config):
    path = get_output_dir(config.workdir) / "karps" / table_metadata_file
    with open(path, "rb") as fp:
        return pickle.load(fp)

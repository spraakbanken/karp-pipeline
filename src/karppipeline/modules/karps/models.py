from pathlib import Path
from pydantic import BaseModel

from karppipeline.models import MultiLang


class Tag(BaseModel):
    label: MultiLang
    description: MultiLang


class FieldMetadata(BaseModel):
    """
    Used to populate translations and value fields in the Karps backend configs
    """

    name: MultiLang
    description: MultiLang | None = None
    # values are used by enums to validate that the given values are in the set and also for translation
    values: dict[str, MultiLang] = {}


class EntryWord(BaseModel):
    # description of the entry word field
    field: str
    description: MultiLang


class KarpsConfig(BaseModel):
    # the directory where pipeline should put new resources
    output_config_dir: Path
    cli_path: Path
    # this both decides where to put files and where the cli is
    config_host: str | None = None
    db_database: str
    db_host: str | None = None
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

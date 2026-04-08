from typing import Sequence
from pydantic import BaseModel, Field

from karppipeline.models import NonEmptyMultiLang


DATE_PATTERN: str = r"^(\d{4}-\d{2}-\d{2}|)$"


class MetadataAttributes(BaseModel):
    # these will *overwrite*  the values from the metadata API
    downloads: Sequence[dict[str, str]] = ()
    interfaces: Sequence[str] = ()
    created: str | None = Field(None, pattern=DATE_PATTERN)
    updated: str | None = Field(None, pattern=DATE_PATTERN)
    # only set if working on a resource that already has a DOI
    doi: str | None = None
    # set in resource pipeline config, name and short_description is taken from pipeline config
    description: NonEmptyMultiLang | None = None
    contact_info: str | dict[str, str | dict[str, str]] | None = None
    unlisted: bool = False
    in_collections: Sequence[str] = ()
    keywords: Sequence[str] = ()
    caveats: NonEmptyMultiLang | None = None
    creators: Sequence[str] = ()  # lastname, firstname
    standard_reference: str | None = None
    other_references: Sequence[str] = ()
    intended_uses: NonEmptyMultiLang | None = None
    language_codes: list[str] = ["swe"]


class Metadata(MetadataAttributes):
    class Config:
        extra = "forbid"

    # should not be copied to metadata file
    yaml_export_path: str
    schema_: str = Field(..., alias="schema")
    license: str = "CC-BY-4.0"

    # fallbacks are used if a value is missing
    fallbacks: MetadataAttributes | None = None


class Data(BaseModel):
    class Config:
        extra = "forbid"

    download_url_template: str
    interface_url_template: str


class SBXRepoConfig(BaseModel):
    class Config:
        extra = "forbid"

    metadata: Metadata
    data: Data

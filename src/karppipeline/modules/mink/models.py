from enum import StrEnum
from typing import Any
from pydantic import BaseModel, Field


class InstallOption(StrEnum):
    KARP_S = "karps"
    KARP_RED = "karp_red"

    def pretty_str(self) -> str:
        if self == InstallOption.KARP_S:
            return "Karp Search"
        else:
            return "Karp red"


class MinkConfig(BaseModel):
    api_key: str
    # this is actually KarpsExportConfig of karppipeline.modules.karps.models but link not needed
    karps: dict[str, Any] = Field(default_factory=dict)
    install: list[InstallOption] = Field(default_factory=lambda: [InstallOption.KARP_RED, InstallOption.KARP_S])

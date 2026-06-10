from typing import TextIO
import yaml
from karppipeline.common import Map


class IndentDumper(yaml.SafeDumper):
    """Customized YAML dumper that indents lists."""

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        """Force indentation."""
        return super().increase_indent(flow)


def dump(obj: object, fp: TextIO, indent: int = 2):
    out = dumps(obj, indent)
    fp.write(out)


def dumps(obj: object, indent: int = 2):
    out = yaml.dump(
        obj, allow_unicode=True, Dumper=IndentDumper, indent=indent, default_flow_style=False, sort_keys=False
    )
    return out


def load(fp) -> Map:
    return yaml.safe_load(fp)


def load_array(fp) -> list[Map]:
    return yaml.safe_load(fp)

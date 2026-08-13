from typing import TextIO
import yaml
from yaml.constructor import ConstructorError
from karppipeline.common import Map


class IndentDumper(yaml.SafeDumper):
    """Customized YAML dumper that indents lists."""

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        """Force indentation."""
        return super().increase_indent(flow)


class UniqueKeyLoader(yaml.SafeLoader):
    pass


def construct_mapping(loader, node, deep=False):
    mapping = {}

    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)

        if key in mapping:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key: {key!r}",
                key_node.start_mark,
            )

        value = loader.construct_object(value_node, deep=deep)
        mapping[key] = value

    return mapping


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    construct_mapping,
)


def dump(obj: object, fp: TextIO, indent: int = 2):
    out = dumps(obj, indent)
    fp.write(out)


def dumps(obj: object, indent: int = 2):
    out = yaml.dump(
        obj, allow_unicode=True, Dumper=IndentDumper, indent=indent, default_flow_style=False, sort_keys=False
    )
    return out


def load(fp) -> Map:
    return yaml.load(fp, UniqueKeyLoader)


def load_array(fp) -> list[Map]:
    return yaml.load(fp, UniqueKeyLoader)

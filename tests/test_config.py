from karppipeline.common import Map
from karppipeline.config import _merge_configs


def test_merge_simple():
    conf1: Map = {"export": {"karps": {}}, "resource_id": "so2009"}
    conf2: Map = {"export": {"karps": {"lol": "lol"}}}

    newconf: Map = _merge_configs(conf1, conf2)
    assert newconf == {"export": {"karps": {"lol": "lol"}}, "resource_id": "so2009"}


def test_merge():
    conf1: Map = {"export": {"karps": {"lol": "lol"}}, "resource_id": "so2009"}
    conf2: Map = {"export": {"karps": {"will be": "saved"}}}

    newconf: Map = _merge_configs(conf1, conf2)

    assert newconf == {
        "export": {"karps": {"lol": "lol", "will be": "saved"}},
        "resource_id": "so2009",
    }


def test_merge_overwrite():
    conf1: Map = {"export": {"karps": {"will be": "overwritten"}}, "resource_id": "so2009"}
    conf2: Map = {"export": {"karps": {"will be": "saved"}}}

    newconf: Map = _merge_configs(conf1, conf2)

    assert newconf == {
        "export": {"karps": {"will be": "saved"}},
        "resource_id": "so2009",
    }


def test_merge_fields():
    """
    Field-array is not overwritten, but concatenated
    """
    conf1: Map = {"fields": [{"name": "field1"}], "inner": {"fields": ["overwritten"]}}
    conf2: Map = {"fields": [{"name": "field2"}], "inner": {"fields": ["saved"]}}

    newconf: Map = _merge_configs(conf1, conf2)

    assert newconf == {"fields": [{"name": "field1"}, {"name": "field2"}], "inner": {"fields": ["saved"]}}

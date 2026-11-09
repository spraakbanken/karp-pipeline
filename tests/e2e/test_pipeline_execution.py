from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
import shutil
import subprocess
from tempfile import mkdtemp

from pytest import fixture
import pytest

from karppipeline.util import yaml


@dataclass
class TestResource:
    __test__ = False
    resource_id: str
    protected_metadata: bool = False
    custom_datatransform: bool = False

    @cached_property
    def resource_path(self) -> Path:
        dir = Path(mkdtemp())
        if self.protected_metadata:
            shutil.copytree(f"tests/e2e/resources/{self.resource_id}_mink", dir, dirs_exist_ok=True)
        else:
            shutil.copytree(f"tests/e2e/resources/{self.resource_id}", dir, dirs_exist_ok=True)
        if self.custom_datatransform:
            shutil.copytree("tests/e2e/plugins", dir / "plugins")
        return dir

    @property
    def output_path(self) -> Path:
        return self.resource_path / "output"

    @property
    def data_path(self) -> Path:
        return self.output_path / f"{self.resource_id}.jsonl"

    def run(self):
        return subprocess.run(
            ["karp-pipeline", "run"],
            cwd=self.resource_path,
            capture_output=True,
        )


@fixture(scope="function")
def resource(request):
    params = getattr(request, "param", None) or {}
    resource = TestResource(resource_id="abc123", **params)
    yield resource

    # clean up after test
    shutil.rmtree(resource.output_path, ignore_errors=True)


def test_stuff(resource):
    ret = resource.run()
    assert ret.returncode == 0

    assert resource.output_path.exists()

    # assert that all modules have data
    assert (resource.output_path / "karps").exists()
    assert _count_entries_sql(resource) == 10
    assert _count_entries_karps_config(resource) == 10

    assert (resource.output_path / "karp_red").exists()
    assert (resource.output_path / "sbxrepo").exists()
    assert _count_entries_metadata_file(resource) == 10

    assert resource.data_path.exists()

    with open(resource.data_path) as fp:
        assert len(fp.readlines()) == 10

    # cleanup test
    subprocess.run(["karp-pipeline", "clean"], cwd=resource.resource_path)
    assert not resource.output_path.exists()


@pytest.mark.parametrize("resource", [{"custom_datatransform": True}], indirect=True)
def test_plugin_data_converter(resource):
    ret = resource.run()
    assert ret.returncode == 0

    # transformation code splits on lemgram so this tests should have one more entry than
    # the ones that does not do transform
    assert _count_entries_sql(resource) == 11
    assert _count_entries_karps_config(resource) == 11

    # but jsonl and repo should still have original number
    with open(resource.data_path) as fp:
        assert len(fp.readlines()) == 10
    assert _count_entries_metadata_file(resource) == 10


@pytest.mark.parametrize("resource", [{"protected_metadata": True}], indirect=True)
def test_namespacing(resource):
    ret = resource.run()
    assert ret.returncode == 0

    with open(resource.output_path / "karps/resource.yaml") as fp:
        karps_backend_conf = yaml.load(fp)
        for field in karps_backend_conf["fields"]:
            assert field["name"].startswith(f"_{resource.resource_id}_")


def _count_entries_sql(resource: TestResource) -> int:
    # Counts inserts into main table, not list values
    count = 0
    with open(resource.output_path / "karps/create.sql") as fp:
        for line in fp.readlines():
            if f"INSERT INTO `{resource.resource_id}`" in line:
                count += 1
    return count


def _count_entries_karps_config(resource: TestResource):
    return _count_yaml(resource.output_path, "karps/resource.yaml", "size")


def _count_entries_metadata_file(resource: TestResource):
    return _count_yaml(resource.output_path, f"sbxrepo/{resource.resource_id}.yaml", "entries")


def _count_yaml(output_path: Path, path: str, key: str):
    # to to parse the number after {key}: as int - key can occur on any level
    prefix = f"{key}: "
    with open(output_path / path) as fp:
        for line in fp:
            split = line.split(prefix, maxsplit=1)
            if len(split) == 2 and split[0].strip() == "":
                return int(split[1])
    raise RuntimeError(f"could not find size in {path}")

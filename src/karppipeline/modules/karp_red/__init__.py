import logging
from pathlib import Path
import shlex
import subprocess
from typing import Final
from typing import NotRequired, TypedDict
from karppipeline.common import PipelineException, create_output_dir, get_output_dir
from karppipeline.execution.dependency import Dependency
from karppipeline.models import ConfiguredField, EntrySchema, PipelineConfig
from karppipeline.util import subprocess as internal_subprocess, yaml
from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)

# import: fetch data from Karp red
# export: generate Karp red backend configuration
# install: create resource and add data into Karp red backend


__all__ = ["export", "install", "dependencies"]

dependencies = [Dependency("jsonl"), Dependency("sbxmetadata", optional=True)]


MODULE_NAME: Final[str] = "karp_red"


class KarpRedConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cli_host: str | None = None
    cli_path: Path
    cli_working_dir: Path
    api_key: str | None = None
    # if there already exists a resource with this resource id - it will be replaced
    replace: bool = False


def export(config: PipelineConfig, module_data, **_kwargs):
    entry_schema: EntrySchema = module_data["schema"]["entry_schema"]
    metadata = module_data.get("sbxmetadata") or {}
    name = metadata.get("name") or config.name and config.name.model_dump()
    if not name:
        raise PipelineException("karp: 'name' missing")
    _create_karp_backend_config(config, entry_schema, name)


def _create_karp_backend_config(config: PipelineConfig, entry_schema: EntrySchema, name: dict[str, str]):
    karp_config = {
        "resource_id": config.resource_id,
        "resource_name": name["swe"],
        "fields": {},
        "protected": config.limited_access,
        "protected_metadata": config.protected_metadata,
    }

    def to_dict(ls: list[ConfiguredField]) -> dict[str, ConfiguredField]:
        return {elem.name: elem for elem in ls}

    def dump(schema, target, configured_fields: dict[str, ConfiguredField]):
        for field_name, field in schema.items():
            dumped = field.asdict()

            conf_field = configured_fields.get(field.name)
            if conf_field and conf_field.label:
                dumped["label"] = conf_field.label.model_dump()
            else:
                dumped["label"] = field.name

            if conf_field and conf_field.params:
                dumped["params"] = conf_field.params

            if field.type == "text":
                dumped["type"] = "string"
            if field.type == "object":
                if conf_field:
                    inner_fields = conf_field.fields
                else:
                    inner_fields = []
                dump(field.fields, dumped["fields"], to_dict(inner_fields))
            del dumped["name"]
            if "categories" in dumped:
                del dumped["categories"]
            target[field_name] = dumped

    dump(entry_schema, karp_config["fields"], to_dict(config.fields))

    output_dir = create_output_dir(config.workdir) / "karp_red"
    output_dir.mkdir(exist_ok=True)
    with open(output_dir / f"{config.resource_id}.yaml", "w") as fp:
        yaml.dump(karp_config, fp)


def install(config: PipelineConfig, uninstall=False, instance=MODULE_NAME):
    if uninstall:
        raise PipelineException("Uninstall not supported for module karp_red")

    config_file = get_output_dir(config.workdir) / "karp_red" / f"{config.resource_id}.yaml"

    # adding a resurce in Karp red is done in three steps
    # creating resource with config
    karp_red_config = KarpRedConfig.model_validate(config.modules[instance])

    if karp_red_config.replace:
        _karp_cli_runner(karp_red_config, ["resource", "delete", config.resource_id, "--force"], check=False)

    _karp_cli_runner(karp_red_config, ["resource", "create", str(config_file)])
    # adding entries
    data_file = get_output_dir(config.workdir) / f"{config.resource_id}.jsonl"
    quoted_resource_id = shlex.quote(config.resource_id)
    _karp_cli_runner(karp_red_config, ["entries", "add", quoted_resource_id, str(data_file)])
    # publish the resource
    _karp_cli_runner(karp_red_config, ["resource", "publish", quoted_resource_id])


def import_(pipeline_config: PipelineConfig, instance=MODULE_NAME):
    """
    Fetches data using the configured instance of karp-cli
    """
    karp_red_config = KarpRedConfig.model_validate(pipeline_config.modules[instance])

    output_dir = Path("output/karp_red")
    output_dir.mkdir(exist_ok=True)
    karp_red_output = output_dir.absolute() / "import.jsonl"

    if karp_red_output.exists():
        logger.info(f"Found {output_dir / 'import.jsonl'}")
    else:
        logger.info("Fetching data from Karp red")
        _karp_cli_entries_export(karp_red_config, pipeline_config.resource_id, karp_red_output)

    source_files = (karp_red_output,)
    suffix = "jsonl"
    return source_files, suffix


class RunKwargs(TypedDict):
    cwd: NotRequired[Path]


def _karp_cli_entries_export(config: KarpRedConfig, resource_id: str, final_output_file: Path):
    quoted_host = ""
    if config.cli_host:
        quoted_host = shlex.quote(config.cli_host)
        # make tmp file, has to end with jsonl for karp-cli to generate JSONL and not JSON
        output_file = subprocess.run(
            ["ssh", "--", quoted_host, "mktemp", "/tmp/tmp.XXXXXX.jsonl"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    else:
        output_file = str(final_output_file)

    # call Karp CLI
    _karp_cli_runner(
        config,
        [
            "entries",
            "export",
            shlex.quote(resource_id),
            "--entry-only",
            "-o",
            output_file,
        ],
    )

    if config.cli_host:
        # get tmp file
        data = subprocess.run(
            ["ssh", "--", quoted_host, f"cat {output_file}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        with open(final_output_file, "w") as fp:
            fp.write(data)

        # remove tmp file
        subprocess.run(["ssh", "--", quoted_host, f"rm -f -- {shlex.quote(output_file)}"])


def _karp_cli_runner(config: KarpRedConfig, karp_args, check=True):
    """
    Run karp-cli on the configured host. Quote args before passing to this function
    """
    karp_cli = config.cli_path
    cwd = config.cli_working_dir

    args = [str(karp_cli), *karp_args]

    kwargs: RunKwargs = {}
    if config.cli_host:
        args = ["ssh", "--", shlex.quote(config.cli_host), " ".join(["cd", shlex.quote(str(cwd)), "&&"] + args)]
    else:
        kwargs = {"cwd": cwd}

    internal_subprocess.run_subprocess(args, check=False, **kwargs)

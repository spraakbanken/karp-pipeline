import logging
from pathlib import Path
import subprocess
from typing import Final
from karppipeline.common import PipelineException, create_output_dir, get_output_dir
from karppipeline.execution.dependency import Dependency
from karppipeline.models import EntrySchema, PipelineConfig
from karppipeline.util import yaml
from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)

# import: fetch data from Karp red
# export: generate Karp red backend configuration
# install: create resource and add data into Karp red backend


__all__ = ["export", "install", "dependencies"]

dependencies = [Dependency("jsonl"), Dependency("sbxmetadata", optional=True)]


MODULE_NAME: Final[str] = "karp"


class KarpRedConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cli_path: Path
    cli_working_dir: Path
    api_key: str | None = None


def export(config: PipelineConfig, module_data, **_kwargs):
    entry_schema: EntrySchema = module_data["schema"]["entry_schema"]
    name = module_data["sbxmetadata"].get("name") or config.name and config.name.model_dump()
    if not name:
        raise PipelineException("karp: 'name' missing")
    _create_karp_backend_config(config, entry_schema, name)


def _create_karp_backend_config(config: PipelineConfig, entry_schema: EntrySchema, name: dict[str, str]):
    karp_config = {"resource_id": config.resource_id, "resource_name": name["swe"], "fields": {}}
    for field_name, field in entry_schema.items():
        dumped = field.asdict()
        if field.type == "text":
            dumped["type"] = "string"
        karp_config["fields"][field_name] = dumped

    output_dir = create_output_dir(config.workdir) / "karp"
    output_dir.mkdir(exist_ok=True)
    with open(output_dir / f"{config.resource_id}.yaml", "w") as fp:
        yaml.dump(karp_config, fp)


def install(config: PipelineConfig, uninstall=False, instance=MODULE_NAME):
    if uninstall:
        raise PipelineException("Uninstall not supported for module karp")

    config_file = get_output_dir(config.workdir) / "karp" / f"{config.resource_id}.yaml"

    # adding a resurce in Karp is done in three steps
    # creating resource with config
    karp_red_config = KarpRedConfig.model_validate(config.modules[instance])

    _karp_cli_runner(karp_red_config, ["resource", "create", str(config_file)])
    # adding entries
    data_file = get_output_dir(config.workdir) / f"{config.resource_id}.jsonl"
    _karp_cli_runner(karp_red_config, ["entries", "add", config.resource_id, str(data_file)])
    # publish the resource
    _karp_cli_runner(karp_red_config, ["resource", "publish", config.resource_id])


def import_(pipeline_config: PipelineConfig, instance=MODULE_NAME):
    """
    Fetches data using the configured instance of karp-cli
    """
    karp_red_config = KarpRedConfig.model_validate(pipeline_config.modules[instance])

    output_dir = Path("output/karp-red")
    output_dir.mkdir(exist_ok=True)
    karp_red_output = output_dir.absolute() / "import.jsonl"

    if karp_red_output.exists():
        logger.info(f"Found {output_dir / 'import.jsonl'}")
    else:
        logger.info("Fetching data from Karp red")
        _karp_cli_runner(
            karp_red_config,
            [
                "entries",
                "export",
                pipeline_config.resource_id,
                "--entry-only",
                "-o",
                karp_red_output,
            ],
        )

    source_files = (karp_red_output,)
    suffix = "jsonl"
    return source_files, suffix


def _karp_cli_runner(config: KarpRedConfig, cmds):
    karp_cli = config.cli_path
    cwd = config.cli_working_dir
    try:
        result = subprocess.run(
            [karp_cli, *cmds],
            check=True,
            capture_output=True,
            text=True,
            cwd=cwd,
        )
        logger.info(f"karp(-red)-cli stdout: {result.stdout.strip()}")
        if result.stderr:
            logger.info(f"karp(-red)-cli stderr: {result.stderr.strip()}")
    except subprocess.CalledProcessError as e:
        logger.error(e.stdout)
        logger.error(e.stderr)
        raise

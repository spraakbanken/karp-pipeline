from pathlib import Path
import shlex
import shutil
import subprocess

from karppipeline.common import get_output_dir, InstallException
from karppipeline.logging import get_logger
from karppipeline.modules.karps.models import KarpsConfig
from karppipeline.models import PipelineConfig
from karppipeline.util import yaml

logger = get_logger(__name__, "Karp-s installer")


def _run_subprocess(cmd: str | list[str], err_msg=None, check=True, shell=False, print_output=True) -> int:
    logger.debug(f"Running subprocess: {cmd}")
    p = subprocess.run(cmd, check=False, capture_output=True, shell=shell, encoding="utf-8")
    out = p.stdout
    err = p.stderr
    if print_output:
        if out:
            logger.debug(out)
        if err:
            logger.error(err)
    if check and p.returncode:
        raise InstallException(err_msg)
    return p.returncode


def _rm_files_and_replace_parent(dir_to_replace: Path, files_to_remove: list[Path], host=None):
    """
    Removes the given files and then the given dir if it is empty
    """
    if not host:
        for file_to_remove in files_to_remove:
            file_to_remove.unlink(missing_ok=True)
        if dir_to_replace.exists():
            dir_to_replace.rmdir()
        dir_to_replace.mkdir()
    else:
        cmds = []
        for file_to_remove in files_to_remove:
            cmds.append(f"rm -f {str(file_to_remove)}")
        resource_dir_str = str(dir_to_replace)
        cmds.append(f"rmdir -- {resource_dir_str} 2>/dev/null")
        cmds.append(f"mkdir {resource_dir_str}")
        _run_subprocess(
            f"ssh {shlex.quote(host)} {shlex.quote(f'{"; ".join(cmds)}')}",
            err_msg=f"Unable to create resource directory on host {host}",
            shell=True,
        )


def add_to_db(pipeline_config: PipelineConfig, karps_config: KarpsConfig):
    """
    if karps.db_host is set, ssh + mysql will be used, else only mysql
    """
    host = karps_config.db_host
    db_name = karps_config.db_database

    sqlfile = get_output_dir(pipeline_config.workdir) / f"{pipeline_config.resource_id}.sql"
    logger.info("Installing MySQL database: %s, source: %s", db_name, sqlfile)
    if not host:
        cmd = f"mysql {shlex.quote(db_name)}"
    else:
        cmd = f"ssh {shlex.quote(host)} {shlex.quote(f'mysql {db_name}')}"
    _run_subprocess(
        f"cat {shlex.quote(str(sqlfile))} | {cmd}", shell=True, err_msg="Unable to install database file to Karp-s"
    )


def add_config(pipeline_config: PipelineConfig, karps_config: KarpsConfig, resource_id: str):
    """
    Moves the generated configuration file into a directory for incoming resources for Karp-s (must be configured in backend)
    if `karps_config.host` is set, all steps will be done on host using SSH
    """
    output_dir = get_output_dir(pipeline_config.workdir)
    resource_dir = Path(karps_config.output_config_dir) / resource_id

    host = karps_config.config_host

    resource_file_path = resource_dir / "resource.yaml"
    resource_fields_file_path = resource_dir / "fields.yaml"
    resource_global_file_path = resource_dir / "global.yaml"
    _rm_files_and_replace_parent(
        resource_dir, [resource_file_path, resource_fields_file_path, resource_global_file_path], host=host
    )

    # copy the needed files to the backends config dir
    resource_config = output_dir / f"{resource_id}_karps.yaml"
    fields_config = output_dir / "fields.yaml"

    if not host:
        shutil.copy(resource_config, resource_file_path)
        shutil.copy(fields_config, resource_fields_file_path)
        # move this code to export?
        with open(resource_global_file_path, "w") as fp:
            yaml.dump(
                {"tags_description": {key: val.model_dump() for key, val in karps_config.tags_description.items()}}, fp
            )
    else:
        _run_subprocess(
            ["scp", str(resource_config), f"{host}:{str(resource_dir / f'{resource_id}.yaml')}"],
            err_msg=f"Unable to copy file to host {host}",
        )

    # run the backend cli to process the added files
    cmd = f"{karps_config.cli_path} add {resource_id}"
    if host:
        cmd = f'ssh {shlex.quote(host)} "{cmd}"'
    logger.info("Calling karp-s-cli to add configuration.")
    _run_subprocess(cmd, shell=True, err_msg="karp-s-cli error")

    # try to reload the Karp-s backend (ok to fail, so don't print output from command and only write a warning)
    cmd = f"{karps_config.cli_path} reload"
    if host:
        cmd = f'ssh {shlex.quote(host)} "{cmd}"'
    logger.info("Calling karp-s-cli to reload configuration.")
    return_code = _run_subprocess(cmd, check=False, shell=True, print_output=False)
    if return_code:
        logger.warning("karp-s-backend may not have loaded the new resource")
    else:
        logger.info("karp-s-backend reloaded")

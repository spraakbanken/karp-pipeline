from pathlib import Path
import shlex
import shutil


from karppipeline.common import get_output_dir
from karppipeline.logging import get_logger
from karppipeline.models import PipelineConfig
from karppipeline.modules.karps.models import KarpsInstallConfig
from karppipeline.util.subprocess import run_subprocess

logger = get_logger(__name__, "Karp-s installer")


def _rm_files_and_replace_parent(dir_to_replace: Path, files_to_remove: list[Path], host=None):
    """
    Removes the given files and then the given dir if it is empty
    """
    if not host:
        for file_to_remove in files_to_remove:
            file_to_remove.unlink(missing_ok=True)
        if not dir_to_replace.exists():
            dir_to_replace.mkdir()
    else:
        cmds = []
        for file_to_remove in files_to_remove:
            cmds.append(f"rm -f {str(file_to_remove)}")
        resource_dir_str = str(dir_to_replace)
        cmds.append(f"rmdir -- {resource_dir_str} 2>/dev/null")
        cmds.append(f"mkdir {resource_dir_str}")
        run_subprocess(
            f"ssh {shlex.quote(host)} {shlex.quote(f'{"; ".join(cmds)}')}",
            err_msg=f"Unable to create resource directory on host {host}",
            shell=True,
        )


def add_config(pipeline_config: PipelineConfig, karps_config: KarpsInstallConfig, resource_id: str):
    """
    Moves the generated configuration file into a directory for incoming resources for Karp-s (must be configured in backend)
    if `karps_config.host` is set, all steps will be done on host using SSH
    """
    host = karps_config.config_host
    output_dir = get_output_dir(pipeline_config.workdir) / "karps"
    karps_config_dir = Path(karps_config.output_config_dir)

    # ensure that the karps incoming config directory exists
    if not host:
        karps_config_dir.mkdir(exist_ok=True)
    else:
        cmd = f'ssh {shlex.quote(host)} "mkdir -p {karps_config_dir}"'
        run_subprocess(
            cmd,
            shell=True,
            err_msg=f"Unable to create output directory on host {host}",
        )

    resource_dir = karps_config_dir / resource_id
    resource_file_path = resource_dir / "resource.yaml"
    resource_fields_file_path = resource_dir / "fields.yaml"
    resource_global_file_path = resource_dir / "global.yaml"
    _rm_files_and_replace_parent(
        resource_dir, [resource_file_path, resource_fields_file_path, resource_global_file_path], host=host
    )

    # copy the needed files to the backends config dir
    resource_config = output_dir / "resource.yaml"
    fields_config = output_dir / "fields.yaml"
    global_config = output_dir / "global.yaml"

    for source, target in [
        (resource_config, resource_file_path),
        (fields_config, resource_fields_file_path),
        (global_config, resource_global_file_path),
    ]:
        if not host:
            shutil.copy(source, target)
        else:
            run_subprocess(
                ["scp", str(source), f"{host}:{str(target)}"],
                err_msg=f"Unable to copy file to host {host}",
            )

    # run the backend cli to process the added files
    cmd = f"{karps_config.cli_path} add {resource_id}"
    if host:
        cmd = f'ssh {shlex.quote(host)} "{cmd}"'
    logger.info("Calling karp-s-cli to add configuration.")
    run_subprocess(cmd, shell=True, err_msg="karp-s-cli error")

    # try to reload the Karp-s backend (ok to fail, so don't print output from command and only write a warning)
    cmd = f"{karps_config.cli_path} reload"
    if host:
        cmd = f'ssh {shlex.quote(host)} "{cmd}"'
    logger.info("Calling karp-s-cli to reload configuration.")
    return_code = run_subprocess(cmd, check=False, shell=True, print_output=False)
    if return_code:
        logger.warning("karp-s-backend may not have loaded the new resource")
    else:
        logger.info("karp-s-backend reloaded")


def remove_config(karps_config: KarpsInstallConfig, resource_id: str):
    # try to reload the Karp-s backend (ok to fail, so don't print output from command and only write a warning)
    cmd = f"{karps_config.cli_path} remove {resource_id}"
    host = karps_config.config_host
    if host:
        cmd = f'ssh {shlex.quote(host)} "{cmd}"'

    logger.info(f"Calling karp-s-cli to remove configuration for {resource_id}.")
    return_code = run_subprocess(cmd, check=False, shell=True, print_output=False)
    if return_code:
        logger.error("karp-s-backend may not have removed the resource.")
    else:
        logger.info("karp-s-backend removed resource")

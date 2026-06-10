import shlex

from karppipeline.common import get_output_dir
from karppipeline.logging import get_logger
from karppipeline.modules.karps.models import KarpsInstallConfig
from karppipeline.models import PipelineConfig
from karppipeline.util.subprocess import run_subprocess

logger = get_logger(__name__, "Karp-s installer")


def remove_from_db(pipeline_config: PipelineConfig, karps_config: KarpsInstallConfig):
    _run_db(pipeline_config, karps_config, "delete.sql")


def add_to_db(pipeline_config: PipelineConfig, karps_config: KarpsInstallConfig):
    _run_db(pipeline_config, karps_config, "create.sql")


def _run_db(pipeline_config: PipelineConfig, karps_config: KarpsInstallConfig, sql_file):
    """
    if karps.db_host is set, ssh + mysql will be used, else only mysql
    """
    host = karps_config.db_host
    db_name = karps_config.db_database

    sqlfile = get_output_dir(pipeline_config.workdir) / "karps" / sql_file
    if host:
        host_logging = f", on host {host}"
    else:
        host_logging = ""
    logger.info("Running MySQL database: %s, source: %s%s", db_name, sqlfile, host_logging)
    if not host:
        cmd = f"mysql {shlex.quote(db_name)}"
    else:
        cmd = f"ssh {shlex.quote(host)} {shlex.quote(f'mysql {db_name}')}"
    run_subprocess(
        f"cat {shlex.quote(str(sqlfile))} | {cmd}",
        shell=True,
        err_msg="Unable to run database file for Karp-s install/uninstall",
    )

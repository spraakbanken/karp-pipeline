from pathlib import Path
import shlex

from karppipeline.common import get_output_dir
from karppipeline.logging import get_logger
from karppipeline.modules.karps.models import KarpsInstallConfig, TableMetadata
from karppipeline.models import PipelineConfig
from karppipeline.util.subprocess import run_subprocess

logger = get_logger(__name__, "Karp-s installer")


def remove_from_db(pipeline_config: PipelineConfig, karps_config: KarpsInstallConfig):
    _run_db(pipeline_config, karps_config, "delete.sql")


def add_to_db(pipeline_config: PipelineConfig, karps_config: KarpsInstallConfig, tables_metadata: list[TableMetadata]):
    _run_db(pipeline_config, karps_config, "create.sql")

    for table_metadata in tables_metadata:
        _run_csv(pipeline_config, karps_config, table_metadata)

    _run_db(pipeline_config, karps_config, "indices.sql")


def _run_csv(pipeline_config: PipelineConfig, karps_config: KarpsInstallConfig, table_metadata: TableMetadata):
    """
    Using LOAD DATA LOCAL INFILE, inserts data from a CSV file
    """
    sql = (
        "LOAD DATA LOCAL INFILE '/dev/stdin' "
        f"INTO TABLE `{table_metadata.table}` "
        "CHARACTER SET utf8mb4 "
        "FIELDS TERMINATED BY '\\t' "
        "LINES TERMINATED BY '\\n' " + table_metadata.columns_str
    )
    expr = f" -e {shlex.quote(sql)}"
    _run_db(pipeline_config, karps_config, table_metadata.csv_file, expr=expr)


def _run_db(pipeline_config: PipelineConfig, karps_config: KarpsInstallConfig, file: Path | str, expr: str = ""):
    """
    if karps.db_host is set, ssh + mysql will be used, else only mysql
    """
    host = karps_config.db_host
    db_name = karps_config.db_database

    sqlfile = get_output_dir(pipeline_config.workdir) / "karps" / file
    if host:
        host_logging = f", on host {host}"
    else:
        host_logging = ""
    logger.info("Running MySQL database: %s, source: %s%s", db_name, sqlfile, host_logging)

    cmd = f"mysql {shlex.quote(db_name)}{expr}"
    if host:
        cmd = f"ssh {shlex.quote(host)} {shlex.quote(cmd)}"
    run_subprocess(
        f"cat {shlex.quote(str(sqlfile))} | {cmd}",
        shell=True,
        err_msg="Unable to run database file for Karp-s install/uninstall",
    )

from pathlib import Path

from karppipeline.execution.install import DRY_RUN
from karppipeline.util.git import GitRepo


def log_latest_commit_id(logger) -> None:
    """
    By finding the root directory, call git to get the current commit ID and print it
    If finding the commmit ID with this method fails, print nothing.
    """
    # by using a reference to this file, find the root dir. util -> karppipeline -> src -> root
    pipeline_code_dir = Path(__file__).resolve().parent.parent.parent.parent

    repo = GitRepo(pipeline_code_dir)
    commit = repo.latest_commit()
    if commit:
        logger.info(f"karp-pipeline version: commit {commit}")


def dry_run_info(logger, str) -> None:
    """
    Always log given str, but add warning about dry run if needed
    """
    if DRY_RUN:
        logger.info(str + " (not done because of --dry-run/-n)")
    else:
        logger.info(str)

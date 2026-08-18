from pathlib import Path

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

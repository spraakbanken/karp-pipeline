import subprocess

from karppipeline.execution.install import DRY_RUN


class GitRepo:
    def __init__(self, repo_path):
        self.repo_path = repo_path

    def _run(self, *args, modifying=True):
        if not DRY_RUN or not modifying:
            result = subprocess.run(
                ["git", *args],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                if "nothing to commit" not in result.stdout:
                    raise RuntimeError("Error when calling Git", result.stdout + ", " + result.stderr)

            return result.returncode, result.stdout.strip()
        else:
            return 0, ""

    def init(self):
        self._run("init")
        self._run("commit", "--message", "init", "--allow-empty")

    def pull(self):
        self._run("pull")

    def commit_all(self, msg=None, allow_empty=True):
        self._run("add", "--all")
        commit_args = []
        if allow_empty:
            commit_args.append("--allow-empty")
        self._run("commit", *commit_args, "--message", msg)

    def latest_commit(self) -> str | None:
        returncode, stdout = self._run("rev-parse", "--short", "HEAD", modifying=False)
        if returncode != 0:
            return None
        else:
            return stdout

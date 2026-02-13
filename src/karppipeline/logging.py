from datetime import datetime
import logging
import sys

from karppipeline.common import create_log_dir


logger = logging.getLogger("karppipeline")


def format(date) -> str:
    return date.strftime("%Y-%m-%d %H:%M:%S,%f")


def setup_resource_logging(path, compact_output=False, json_output=False):
    # remove previous handlers
    logger.handlers.clear()

    logger.setLevel("INFO")

    # either log to file or console
    if compact_output:
        # create log file for resource if it does not exist
        log_dir = create_log_dir(path)
        log_file = log_dir / "run.log"
        # write header to know if a new run has started
        with open(log_file, "a") as f:
            f.write("-------------------------------\n")
            f.write(f"pipeline run, {format(datetime.now())}\n")
        handler = logging.FileHandler(log_file)
    else:
        handler = logging.StreamHandler(stream=sys.stdout)

    if json_output:
        import json

        class JsonFormatter(logging.Formatter):
            def format(self, record):
                payload = {
                    "timestamp": format(datetime.fromtimestamp(record.created)),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                }
                if record.exc_info:
                    payload["exc_info"] = self.formatException(record.exc_info)
                return json.dumps(payload)

        formatter = JsonFormatter()
        handler.setFormatter(formatter)

    logger.addHandler(handler)


def get_logger():
    return logger

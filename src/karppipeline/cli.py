import argparse
from typing import TYPE_CHECKING

from karppipeline.common import ImportException, InstallException
from karppipeline.util.terminal import bold, green_box, red_box


if TYPE_CHECKING:
    from karppipeline.config import ConfigHandle


def clean(configs: list["ConfigHandle"]) -> None:
    import shutil
    import os
    from karppipeline.common import get_log_dir, get_output_dir

    """
    remove log and output directories for the given resources
    """
    for resource in configs:
        clean_paths = get_log_dir(resource.workdir), get_output_dir(resource.workdir)
        for path in clean_paths:
            if os.path.exists(path):
                print(f"Remove {path}")
                shutil.rmtree(path)


def cli():
    parser = argparse.ArgumentParser(
        prog="karp-pipeline",
        description="""
        Automatically picks up a config.yaml in current directory, 
        checks for parents and children and runs the command on all 
        resources this level and below.""",
    )

    subparsers = parser.add_subparsers(dest="command", required=True, metavar="")
    subparsers.metavar = "COMMAND"

    subparsers.add_parser("clean", help="remove genereated files")

    def add_modules(p: argparse.ArgumentParser):
        p.add_argument(
            "modules",
            nargs="*",
            help="Modules to invoke (default is to run modules resource config export.default/install)",
        )

    p_run = subparsers.add_parser(
        "run",
        description=f"Prepares the material from /source and places the resulting artifacts in {bold('/output')}. Does not have any side effects except creating files.",
        help="prepares the material",
    )
    add_modules(p_run)

    p_install = subparsers.add_parser(
        "install",
        description="Using the generated artifacts in /output, adds the materials to the requested system. Does not modify /output.",
        help="adds the material to the requested system",
    )
    add_modules(p_install)

    args = parser.parse_args()

    # If help was invoked, parse_args will exit. Imports go after parse_args so that help is generated as fast as possible
    import logging
    from karppipeline.config import find_configs, load_config
    from karppipeline.install import install
    from karppipeline.run import run
    import karppipeline.logging as karps_logging

    configs = find_configs()

    if args.command == "clean":
        clean(configs)
        return 0

    do_run = args.command == "run"
    do_install = args.command == "install"

    kwargs = {}
    if len(args.modules) > 0:
        kwargs["subcommand"] = args.modules

    silent = False
    if len(configs) > 1:
        silent = True
    for config_handle in configs:
        karps_logging.setup_resource_logging(config_handle.workdir, silent=silent)
        try:
            config = load_config(config_handle)
            # run calls importers and exporters
            if not silent:
                if do_run:
                    task_output = "Running"
                elif do_install:
                    task_output = "Installing"
                else:
                    task_output = "Unknown action"
                print(task_output, config.resource_id)
            if do_install:
                install(config, **kwargs)
            elif do_run:
                run(config, **kwargs)
            if silent:
                # TODO inform user if there was warnings
                print(f"{green_box()} {config.resource_id}\t success")
        except Exception as e:
            if isinstance(e, InstallException) or isinstance(e, ImportException):
                logging.getLogger("karppipeline").error(f"Exception for resource: {e.args[0]}")
            else:
                logging.getLogger("karppipeline").error("Exception for resource", exc_info=True)
            if silent:
                print(f"{red_box()} {config_handle.workdir}\t fail")

    return 0

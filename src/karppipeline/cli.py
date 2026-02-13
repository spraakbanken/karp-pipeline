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


def parse_args() -> argparse.Namespace:
    """
    Defines the pipeline CLI input, writes any requested help text and parses the input.
    Exits if --help was invoked.
    """
    parser = argparse.ArgumentParser(
        prog="karp-pipeline",
        # TODO I haven't figured out a way to make Argparse both respect newlines AND not do breaks inside words
        # without writing extra code, use bold * to make some separation between paragraphs.
        description=f"""{bold("*")} Automatically picks up a config.yaml in current directory,
        checks for parents and children and runs the command on all
        resources this level and below.

        {bold("*")} By default, if a command is invoked on multiple resources, the output
        will be one row per resource and the verbose output will be redirected
        to <resource_dir>/log/run.log. If a command is invoked on a single resource,
        the output will be written to stdout. To override this
        behavior use --no-compact or --compact. In addition,to switch from
        human-readable output to JSON, use --json-output. The format of the output
        does not affect verbosity or compactness.""",
    )

    subparsers = parser.add_subparsers(dest="command", required=True, metavar="")
    subparsers.metavar = "COMMAND"

    subparsers.add_parser("clean", help="remove genereated files")

    def add_output_params(p: argparse.ArgumentParser):
        group = p.add_mutually_exclusive_group()
        group.add_argument(
            "--no-compact",
            help="Write all output to console. This is the default when running a single resource.",
            action="store_const",
            const="no_compact",
            dest="compact_output",
        )
        group.add_argument(
            "--compact",
            help="Write one line per resource to console. This is the default when running a multiple resources.",
            action="store_const",
            const="compact",
            dest="compact_output",
        )
        p.set_defaults(compact_output="default")
        p.add_argument(
            "--json-output",
            help="Write output as one line of json per logging event.",
            action="store_true",
            default=False,
        )
        p.add_argument(
            "--log-level",
            default="INFO",
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            type=str.upper,
            help="Use DEBUG,INFO,WARNING or ERROR (default: INFO)",
        )

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
    add_output_params(p_run)

    p_install = subparsers.add_parser(
        "install",
        description="Using the generated artifacts in /output, adds the materials to the requested system. Does not modify /output.",
        help="adds the material to the requested system",
    )
    add_modules(p_install)
    add_output_params(p_install)

    return parser.parse_args()


def cli():
    args = parse_args()

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

    compact_output = False
    if args.compact_output == "default":
        if len(configs) > 1:
            compact_output = True
    else:
        compact_output = args.compact_output == "compact"
    for config_handle in configs:
        karps_logging.setup_resource_logging(
            config_handle.workdir, args.log_level, compact_output=compact_output, json_output=args.json_output
        )
        logger = logging.getLogger(__name__)
        try:
            config = load_config(config_handle)
            # run calls importers and exporters
            if not compact_output:
                if do_run:
                    task_output = "Running "
                elif do_install:
                    task_output = "Installing "
                else:
                    task_output = "Unknown action"

                logger.info(task_output + config.resource_id)
            if do_install:
                install(config, **kwargs)
            elif do_run:
                run(config, **kwargs)
            if compact_output:
                # TODO inform user if there was warnings
                print(f"{green_box()} {config.resource_id}\t success")
        except Exception as e:
            if isinstance(e, InstallException) or isinstance(e, ImportException):
                logging.getLogger("karppipeline").error(f"Exception for resource: {e.args[0]}")
            else:
                logging.getLogger("karppipeline").error("Exception for resource", exc_info=True)
            if compact_output:
                print(f"{red_box()} {config_handle.workdir}\t fail")

    return 0

# equity_aggregator/cli.py

import argparse
import asyncio
import sys

from .domain import aggregate_canonical_equities as aggregate
from .domain import download_canonical_equities as download
from .logging_config import configure_logging
from .storage import export_canonical_equities as export


def run_command(fn: callable) -> None:
    """
    Executes an asynchronous command function using asyncio.

    Runs the provided coroutine function and handles exceptions by printing
    the error to stderr and exiting with status code 1.

    Args:
        fn (callable): An asynchronous function to execute.

    Returns:
        None
    """
    try:
        asyncio.run(fn())
    except Exception as exc:
        print(f"{exc.__class__.__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from None


def main() -> None:
    """
    Entry point for the equity-aggregator CLI application.

    Configures logging, sets up command-line argument parsing, and dispatches
    execution to the appropriate subcommand handler based on user input.

    Args:
        None

    Returns:
        None
    """
    configure_logging()

    parser = argparse.ArgumentParser(prog="equity-aggregator")

    # add required subcommands
    sub = parser.add_subparsers(dest="cmd", required=True)

    # add aggregate subcommand
    sub.add_parser("aggregate")

    # add export subcommand
    sub.add_parser("export")

    # add download subcommand
    sub.add_parser("download")

    # read args from command line
    args = parser.parse_args()

    if args.cmd == "aggregate":
        run_command(aggregate)
        return

    if args.cmd == "export":
        export()
        return

    if args.cmd == "download":
        run_command(download)
        return

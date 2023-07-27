"""
Command line interface 
"""

import argparse
import sys

from kaeru import __title__, __version__
from kaeru.cli_utils import node_processing_pipeline, relation_processing_pipeline


def get_parser():
    """Parse command line arguments and return a parser"""
    parser = argparse.ArgumentParser(description="Command-line interface for kaeru")

    parser.add_argument(
        "-v",
        "--version",
        help="show version information and exit",
        action="version",
        version=f"{__title__} {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command", help="commands")

    # node command
    node_parser = subparsers.add_parser(
        "node", help="Generate Datalog node EDBs from Neo4j node data"
    )
    node_parser.add_argument(
        "-t",
        "--type",
        help="Specify the type of node EDB to generate",
        choices=["fact", "schema", "all"],
        required=True,
    )
    node_parser.add_argument(
        "-l", "--label", help="Specify node label name", required=True
    )
    node_parser.add_argument(
        "-f",
        "--file",
        help="Name of the input Neo4j Node file. Support csv file for now",
        required=True,
    )
    node_parser.add_argument(
        "-o",
        "--output",
        help="Directory for node EDB output. Default to current working directory.",
    )
    node_parser.add_argument(
        "-d",
        "--directory",
        help="Directory for input node file. Default to current working directory.",
    )
    node_parser.add_argument(
        "-s",
        "--storage",
        default="row",
        choices=["row", "col"],
        help="Specify storage type for node EDB. Support row-based and column-based storage.",
    )

    # relation command
    rel_parser = subparsers.add_parser(
        "relation",
        help="Generate Datalog relationship EDBs from Neo4j relationship data",
    )
    rel_parser.add_argument(
        "-t",
        "--type",
        help="Specify the type of relationship EDB to generate",
        choices=["fact", "schema", "all"],
        required=True,
    )
    rel_parser.add_argument(
        "-l", "--label", help="Specify relationship type name", required=True
    )
    rel_parser.add_argument(
        "-f",
        "--file",
        help="Name of the input Neo4j relationship file. Support csv file for now",
        required=True,
    )
    rel_parser.add_argument(
        "-o",
        "--output",
        help="Directory for relationship EDB output. Default to current working directory.",
    )
    rel_parser.add_argument(
        "-d",
        "--directory",
        help="Directory for input relationship file. Default to current working directory.",
    )

    return parser


def process_args(parser, args):
    """Perform property graph data to Datalog data conversion given cli arguments"""

    args = parser.parse_args(args)

    if not args.command:
        parser.print_help()
        exit()

    if args.command == "node":
        node_processing_pipeline(args)
        exit()

    if args.command == "relation":
        relation_processing_pipeline(args)
        exit()

    return


def main():
    """Entry point"""
    parser = get_parser()
    process_args(parser, sys.argv[1:])

"""
Command line interface 
"""

import argparse
import sys

from kaeru import __title__, __version__
from kaeru.cli_utils import node_processing_pipeline



def get_parser():
    """ Parse command line arguments and return a parser """
    parser = argparse.ArgumentParser(description="Command-line interface for kaeru")
    
    # global options
    parser.add_argument("-t", "--type", 
                      help="specify the type of outputs", 
                      choices=["fact", "schema", "all"],
                      required=True)
    
    parser.add_argument("-l", "--label",
                      help="specify label name",
                      required=True)
    
    parser.add_argument("-f", "--file",
                      help="name of the input Neo4j file with file extension ",
                      required=True)
    
    parser.add_argument("-o", "--output",
                      help="directory for output")
    
    parser.add_argument("-d", "--directory",
                      help="directory for input files.")
    
    
    parser.add_argument(
        "-v", "--version",
        help="show version information and exit",
        action="version",
        version=f"{__title__} {__version__}"
    )

    # command 
    subparsers = parser.add_subparsers(dest="command")
    # command node
    node_parser = subparsers.add_parser("node", help="generate Datalog node EDBs from Neo4j data")
    # command node specific option 
    node_parser.add_argument("-s", "--storage", default="row", choices=["row", "col"], help="specify storage type for node property")
    
    # command relation
    subparsers.add_parser("relation", help="generate Datalog relationship EDBs from Neo4j data")
    
    return parser


def process_args(parser, args):
    """ Perform property graph data to Datalog data conversion given cli arguments """
    
    args = parser.parse_args(args)    
    
    if not args.command:
        parser.print_help()
        exit()

    if args.command == "node": 
        node_processing_pipeline(args)
        exit()

    if args.command == "relation":
        print("schema command to be implemented")
        exit()

    return



def main():
    """ Entry point """
    parser = get_parser()
    process_args(parser, sys.argv[1:])
    


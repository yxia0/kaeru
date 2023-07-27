"""
Functions dedicated to command-line processing.
"""

import os

from kaeru.node import (createNodeSchema, 
                        createNodes, 
                        writeColumnBasedNodeDeclaration,
                        writeRowBasedNodeDeclaration, 
                        writeColumnBasedNodeIdentifierFacts, 
                        writeColumnBasedNodePropertyFacts, 
                        writeRowBasedNodeFacts)


DEFAULT_DIR = os.getcwd()



def node_schema_process(input_path, output_path, args) -> None:

    input_file = os.path.join(input_path, args.file)
    
    # create schema
    inputFile = open(input_file,"r", encoding="utf-8")
    nodeSchema = createNodeSchema(inputFile, args.label)
    inputFile.close()
    
    # write schema
    output_file = os.path.join(output_path, f"{args.label}_decl.txt")
    outputFile = open(output_file, "w", encoding="utf-8")
    
    if args.storage == "row":
        writeRowBasedNodeDeclaration(nodeSchema, outputFile)
    elif args.storage == "col":
        writeColumnBasedNodeDeclaration(nodeSchema, outputFile)

    outputFile.close()

    return 


def node_fact_process(input_path, output_path, args) -> None:

    input_file = os.path.join(input_path, args.file)
   
    # create schema 
    inputFile = open(input_file,"r", encoding="utf-8")
    nodeSchema = createNodeSchema(inputFile, args.label)
    inputFile.close()

    # create nodes
    inputFile = open(input_file,"r", encoding="utf-8")
    nodeList = createNodes(inputFile,nodeSchema)
    inputFile.close()

    # write nodes
    
    if args.storage == "row":
        for node in nodeList:
            label = node.getLabel()
            output_file = os.path.join(output_path, f"{label}.facts")
            outputFile = open(output_file, "a", encoding="utf-8")
            writeRowBasedNodeFacts(node, nodeSchema, outputFile)
            outputFile.close()
    
    elif args.storage == "col":
        for node in nodeList:
            # write id file 
            label = node.getLabel()
            output_file = os.path.join(output_path, f"{label}.facts")
            outputFile = open(output_file, "a", encoding="utf-8")
            writeColumnBasedNodeIdentifierFacts(node, outputFile)
            outputFile.close()
            # write to property file 
            # rename property if sub labels exist
            propertyNameList = node.getPropertyNames()
            for propertyName in propertyNameList:
                output_file = os.path.join(output_path, f"{propertyName}.facts")
                outputFile = open(output_file, "a", encoding="utf-8")
                writeColumnBasedNodePropertyFacts(node, propertyName, outputFile)
                outputFile.close()

    return 




def node_processing_pipeline(args) -> None:

    if args.directory == None:
        input_path = DEFAULT_DIR
        print(f"file directory is: {input_path}")
    else:
        input_path = args.directory
        print(f"file directory is: {input_path}")

    if args.output == None:
        output_path = DEFAULT_DIR
    else:
        output_path = args.output

    if args.type == "schema":
        node_schema_process(input_path, output_path, args)

    elif args.type == "fact":
        node_fact_process(input_path, output_path, args)
        

    elif args.type == "all":
        node_schema_process(input_path, output_path, args)
        node_fact_process(input_path, output_path, args)

    return 






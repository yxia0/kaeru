"""
Module bundling all functions needed to extract Node schema and facts 
"""


from typing import Set, Mapping, List, Any
import re
from enum import Enum
from collections import OrderedDict


# field type
class NodeType(Enum):
    ID = 1
    LABEL = 2
    PROPERTY = 3


# schema
class NodeSchema:
    def __init__(self):
        self.entryToField = {}
        self.nodeSubLabels = set()
        self.nodeGlobalLabel = None  # group label, only matters for schema
        self.nodeProperty = {}  # a map of tuple (key: position, value : (name, type))
        self.hasSubLabels = False
        self.subLabelsPosition = None

    def addEntry(self, position: int, fieldType: NodeType) -> None:
        self.entryToField[position] = fieldType

    def addProperty(self, position: int, propertyName: str, propertyType: str) -> None:
        self.nodeProperty[position] = (propertyName, propertyType)

    def addSubLabel(self, label: str) -> None:
        self.nodeSubLabels.add(label)

    def setGlobalLabel(self, label: str) -> None:
        self.nodeGlobalLabel = label

    def setSubLabelPosition(self, position: int) -> None:
        self.hasSubLabels = True
        self.subLabelsPosition = position

    def getEntryType(self, position: int) -> NodeType:
        return self.entryToField[position]

    def getPropertyName(self, position: int) -> str:
        return self.nodeProperty[position][0]

    def getNodeSubLabels(self) -> Set[str]:
        return self.nodeSubLabels

    def getNodeGlobalLabel(self) -> str:
        return self.nodeGlobalLabel

    def getPropertyNameAndType(self) -> Mapping:
        return self.nodeProperty

    def getPropertyType(self, propertyName) -> str:
        for _, valueTuple in self.nodeProperty.items():
            if valueTuple[0] == propertyName:
                return valueTuple[1]

        raise Exception(f"property {propertyName} not found in node schema")


class Node:
    def __init__(self):
        self.id = None
        self.label = None  # for printing to which file?
        self.property = OrderedDict()  # an ordered map of property name to its value

    def setId(self, identifier: str) -> None:
        self.id = identifier

    def setLabel(self, label: str) -> None:
        self.label = label

    def setProperty(self, propertyName: str, propertyValue: str) -> None:
        self.property[propertyName] = propertyValue

    def getId(self) -> str:
        return self.id

    def getPropertyValue(self, propertyName: str) -> str:
        return self.property[propertyName]

    def getPropertyNames(self) -> List[str]:
        return list(self.property.keys())

    def getProperty(self) -> Mapping:
        return self.property

    def getLabel(self) -> str:
        return self.label

    def removeProperty(self, propertyName: str) -> None:
        self.property.pop(propertyName)


# --------- Helper functions ---------#
def mapToSouffleType(propertyType):
    if propertyType == "STRING":
        return "symbol"
    elif propertyType == "LONG":
        return "unsigned"
    elif propertyType == "INT":
        return "unsigned"
    else:
        return "symbol"


def createNodeSchema(inputFile: Any, label: str | None) -> NodeSchema:
    """
    Returns a node schema object given an input neo4j data file
    """

    schema = NodeSchema()

    if label:
        schema.setGlobalLabel(label)

    # read header
    fileHeader = inputFile.readline().strip("\n").split("|")
    for position, attribute in enumerate(fileHeader):
        if ":ID" in attribute:
            schema.addEntry(position, NodeType.ID)
            # search if any group label, for example `id:ID(Organisation)``
            groupLabel = re.findall("\(([^)]+)\)", attribute)
            if len(groupLabel) == 1:
                schema.setGlobalLabel(groupLabel[0])
            else:
                raise Exception("Error: too many (LABEL) found in the input data file")

        elif ":LABEL" in attribute:
            schema.addEntry(position, NodeType.LABEL)
            schema.setSubLabelPosition(
                position
            )  # there are sub labels in the file, keep searching the file

        else:
            schema.addEntry(position, NodeType.PROPERTY)

            # get property name and property type
            if ":" in attribute:
                propertyName = attribute.split(":")[0]
                propertyType = mapToSouffleType(attribute.split(":")[1])

            else:
                # property type not specified, defaults to int
                propertyName = attribute
                propertyType = "unsigned"

            schema.addProperty(position, propertyName, propertyType)

    if schema.hasSubLabels:
        # keep scanning data file to collect all sub labels
        labelPos = schema.subLabelsPosition
        for row in inputFile.readlines():
            subLabel = row.strip("\n").split("|")[labelPos]
            schema.addSubLabel(subLabel)

    return schema


def createNodes(inputFile: Any, nodeSchema: NodeSchema) -> List[Node]:
    nodeList = []

    # skip header
    _ = inputFile.readline()

    # loop over rows in the data file
    for row in inputFile.readlines():
        rowData = row.strip("\n").split("|")
        node = Node()

        for position, value in enumerate(rowData):
            if nodeSchema.getEntryType(position) == NodeType.ID:
                # value can not be NULL
                node.setId(value)

            elif nodeSchema.getEntryType(position) == NodeType.LABEL:
                # value can not be NULL
                node.setLabel(value)

            elif nodeSchema.getEntryType(position) == NodeType.PROPERTY:
                # get property name from schema
                # value can be NULL, if type is string, give "NULL", if type is number, give 0
                # temporary workaround
                propertyName = nodeSchema.getPropertyName(position)
                propertyType = nodeSchema.getPropertyType(propertyName)

                if value == "":
                    if propertyType == "symbol":
                        value = "NULL"
                    else:
                        value = 0

                node.setProperty(propertyName, value)

            else:
                raise Exception("Error: Unknown Node Type detected.")

        if node.getLabel() != None:
            # case: sub label exists
            # so rename property name, preserving order
            nodeLabel = node.getLabel()
            propertyNameList = node.getPropertyNames()
            for propertyName in propertyNameList:
                newPropertyName = nodeLabel + propertyName.capitalize()
                propertyValue = node.getPropertyValue(propertyName)
                # remove old property and set newly named property
                node.removeProperty(propertyName)
                node.setProperty(newPropertyName, propertyValue)
        else:
            # set global label as the node label
            node.setLabel(nodeSchema.getNodeGlobalLabel())

        nodeList.append(node)

    return nodeList


# ---------- Property rename helper functions ----------#


# ------- Output Helper functions ------#


def writeColumnBasedNodePropertyDeclHelper(
    nodeSchema: NodeSchema, outputFile: Any
) -> None:
    nodeProperty = nodeSchema.getPropertyNameAndType()
    for _, valueTuple in nodeProperty.items():
        propertyName = valueTuple[0]
        propertyType = valueTuple[1]
        # property rename based on sub labels
        if nodeSchema.hasSubLabels:
            subLabelList = nodeSchema.getNodeSubLabels()
            for subLabel in subLabelList:
                # Rename property name
                newPropertyName = subLabel + propertyName

                outputFile.write(
                    f".decl {newPropertyName}(id:unsigned, {newPropertyName}:{propertyType})\n"
                )
                outputFile.write(
                    f'.input {newPropertyName}(IO=file, filename="{newPropertyName}.facts")\n'
                )
                outputFile.write("\n")

        elif nodeSchema.getNodeGlobalLabel().lower() not in propertyName.lower():
            nodeLabel = nodeSchema.getNodeGlobalLabel()

            propertyName = propertyName.capitalize()

            outputFile.write(
                f".decl {nodeLabel}{propertyName}(id:unsigned, {nodeLabel}{propertyName}:{propertyType})\n"
            )
            outputFile.write(
                f'.input {nodeLabel}{propertyName}(IO=file, filename="{nodeLabel}{propertyName}.facts")\n'
            )
            outputFile.write("\n")

        else:
            outputFile.write(
                f".decl {propertyName}(id:unsigned, {propertyName}:{propertyType})\n"
            )
            outputFile.write(
                f'.input {propertyName}(IO=file, filename="{propertyName}.facts")\n'
            )
            outputFile.write("\n")

    return


def writeColumnBasedNodePropertyUnionDeclHelper(
    nodeSchema: NodeSchema, outputFile: Any
) -> None:
    nodeProperty = nodeSchema.getPropertyNameAndType()
    for _, valueTuple in nodeProperty.items():
        # Get Property name part only
        propertyName = valueTuple[0]
        propertyType = valueTuple[1]
        # property rename based on sub labels
        if nodeSchema.hasSubLabels:
            propertyName = propertyName.capitalize()
            globalLabel = nodeSchema.getNodeGlobalLabel()
            outputFile.write(
                f".decl {globalLabel}{propertyName}(id:unsigned, {propertyName}:{propertyType})\n"
            )
            subLabelList = nodeSchema.getNodeSubLabels()
            for subLabel in subLabelList:
                # if the original property name obtained from input data file
                # uses the global/parent label, rename to sublabel
                outputFile.write(
                    f"{globalLabel}{propertyName}(id, {propertyName}) :- {subLabel}{propertyName}(id, {propertyName}).\n"
                )

        outputFile.write("\n")

    return


def writeColumnBasedNodeIdDeclHelper(nodeSchema: NodeSchema, outputFile: Any) -> None:
    # Node identifier
    if nodeSchema.hasSubLabels:
        nodeLabelSets = nodeSchema.getNodeSubLabels()
    else:
        nodeGlobalLabel = nodeSchema.getNodeGlobalLabel()
        nodeLabelSets = {nodeGlobalLabel}

    for label in nodeLabelSets:
        outputFile.write(f".decl {label}(id:unsigned)\n")
        outputFile.write(f'.input {label}(IO=file, filename="{label}.facts")\n')
        outputFile.write("\n")

    # Node group union of identifier
    if nodeSchema.hasSubLabels:
        globalLabel = nodeSchema.getNodeGlobalLabel()
        subLabels = nodeSchema.getNodeSubLabels()
        outputFile.write(f".decl {globalLabel}(id:unsigned)\n")
        for subLabel in subLabels:
            outputFile.write(f"{globalLabel}(id):- {subLabel}(id).\n")
        outputFile.write("\n")

    return


# ------- Output functions ------#


def writeColumnBasedNodeDeclaration(nodeSchema: NodeSchema, outputFile: Any) -> None:
    """
    Generate column-based Datalog Node schema given Neo4j property graph
    schema and write to a file at filePath.

    Examples of column-based Datalog Node schema:

    .decl City(id:number)
    .decl CityName(id: number, name:symbol)
    .decl CityScore(id: number, score:number)
    """

    # Node identifier
    writeColumnBasedNodeIdDeclHelper(nodeSchema, outputFile)
    # Node property plus property rename
    writeColumnBasedNodePropertyDeclHelper(nodeSchema, outputFile)
    # Add node property union declarations
    writeColumnBasedNodePropertyUnionDeclHelper(nodeSchema, outputFile)

    return


def writeRowBasedNodeDeclaration(nodeSchema: NodeSchema, outputFile: Any) -> None:
    """
    Generate row-based Datalog Node schema given Neo4j property graph
    schema and write to a file at filePath

    Examples of row-based Datalog Node schema:

    .decl City(id:number, cityname:symbol, score: number)
    """

    # Get Node label sets
    if nodeSchema.hasSubLabels:
        nodeLabelSets = nodeSchema.getNodeSubLabels()
    else:
        nodeGlobalLabel = nodeSchema.getNodeGlobalLabel()
        nodeLabelSets = {nodeGlobalLabel}

    nodeProperty = nodeSchema.getPropertyNameAndType()
    for label in nodeLabelSets:
        output = f".decl {label}(id:unsigned"
        for _, valueTuple in nodeProperty.items():
            propertyName = valueTuple[0]
            propertyType = valueTuple[1]
            output += f", {propertyName}:{propertyType}"

        output += ")\n"

        outputFile.write(output)
        outputFile.write(f'.input {label}(IO=file, filename="{label}.facts")\n')
        outputFile.write("\n")

    # Node union sub labels with global labels declaration
    if nodeSchema.hasSubLabels:
        globalLabel = nodeSchema.getNodeGlobalLabel()
        output = f".decl {globalLabel}(id:unsigned"
        for _, valueTuple in nodeProperty.items():
            propertyName = valueTuple[0]
            propertyType = valueTuple[1]
            output += f", {propertyName}:{propertyType}"
        output += ")\n"
        outputFile.write(output)

        # union sublabel
        subLabels = nodeSchema.getNodeSubLabels()

        for subLabel in subLabels:
            output = f"{globalLabel}(id"
            for _, valueTuple in nodeProperty.items():
                propertyName = valueTuple[0]
                output += f", {propertyName}"
            output += f"):- {subLabel}(id"
            for _, valueTuple in nodeProperty.items():
                propertyName = valueTuple[0]
                output += f", {propertyName}"
            output += ").\n"
            outputFile.write(output)

        outputFile.write("\n")

    return


def writeColumnBasedNodeIdentifierFacts(node: Node, outputFile: Any) -> None:
    """
    Generate column-based Datalog Node facts given Neo4j property graph
    data and write to a file at filePath.
    """

    identifier = node.getId()
    outputFile.write(identifier + "\n")

    return


def writeColumnBasedNodePropertyFacts(
    node: Node, propertyName: str, outputFile: Any
) -> None:
    """
    Generate column-based Datalog Node facts given Neo4j property graph
    data and write to a file at filePath.
    """

    propertyValue = node.getPropertyValue(propertyName)
    identifier = node.getId()
    outputFile.write(identifier + "\t" + propertyValue + "\n")

    return


def writeRowBasedNodeFacts(node: Node, nodeSchema: NodeSchema, outputFile: Any) -> None:
    """
    Generate row-based Datalog Node facts given Neo4j property graph
    date and write to a file at filePath
    """

    identifier = node.getId()
    propertyMapping = node.getProperty()
    output = identifier

    for _, value in propertyMapping.items():
        output += "\t" + value
    output += "\n"

    outputFile.write(output)

    return

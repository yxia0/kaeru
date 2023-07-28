"""
Module bundling all functions needed to extract relationship schema and facts 
"""
from enum import Enum
from collections import OrderedDict
import re

from typing import Any, List, Mapping


# field type
class RelationType(Enum):
    START_ID = 1
    END_ID = 2
    PROPERTY = 3


class RelationSchema:
    def __init__(self):
        self.entryToField = {}
        self.relationGlobalLabel = None
        self.relationProperty = {}
        self.sourceName = {}  # name/type of start and end node

    def addEntry(self, position: int, fieldType: RelationType) -> None:
        self.entryToField[position] = fieldType

    def addProperty(self, position: int, propertyName: str, propertyType: str) -> None:
        self.relationProperty[position] = (propertyName, propertyType)

    def setGlobalLabel(self, label: str) -> None:
        self.relationGlobalLabel = label

    def getEntryType(self, position: int) -> RelationType:
        return self.entryToField[position]

    def setSourceName(self, name: str) -> None:
        self.sourceName["start"] = name

    def setTargetName(self, name: str) -> None:
        self.sourceName["target"] = name

    def getSourceName(self) -> str:
        return self.sourceName["start"]

    def getTargetName(self) -> str:
        return self.sourceName["target"]

    def getGlobalLabel(self) -> str:
        return self.relationGlobalLabel

    def getPropertyNames(self) -> List[str]:
        positionList = list(self.relationProperty.keys())
        positionList.sort()

        return [self.relationProperty[key][0] for key in positionList]

    def getPropertyName(self, position: int) -> str:
        return self.relationProperty[position][0]

    def getPropertyTypeByName(self, propertyName: str) -> str:
        for _, valueTuple in self.relationProperty.items():
            if valueTuple[0] == propertyName:
                return valueTuple[1]

        raise Exception(f"property {propertyName} not found in relation schema")

    def getPropertyTypeByPosition(self, position: int) -> str:
        return self.relationProperty[position][1]


class Relation:
    def __init__(self):
        self.label = None
        self.startId = None
        self.endId = None
        self.property = OrderedDict()

    def setLabel(self, label: str) -> None:
        self.label = label

    def setSourceId(self, identifier: int) -> None:
        self.startId = identifier

    def setTargetId(self, identifier: int) -> None:
        self.endId = identifier

    def setProperty(self, propertyName: str, propertyValue: str) -> None:
        self.property[propertyName] = propertyValue

    def getLabel(self) -> str:
        return self.label

    def getSourceId(self) -> str:
        return self.startId

    def getTargetId(self) -> str:
        return self.endId

    def getProperty(self) -> Mapping:
        return self.property


# ------------ functions for creating schema and fact object -------#


def mapToSouffleType(propertyType):
    if propertyType == "STRING":
        return "symbol"
    elif propertyType == "LONG":
        return "unsigned"
    elif propertyType == "INT":
        return "unsigned"
    else:
        return "symbol"


def createRelationSchema(inputFile: Any, label: str | None) -> RelationSchema:
    """
    Returns a relation schema object given an Neo4j input relation data file
    """

    # Create
    schema = RelationSchema()
    if label:
        schema.setGlobalLabel(label)
    else:
        raise Exception("Error: please supply relation label through cli option.")

    # Read header from input file
    fileHeader = inputFile.readline().strip("\n").split("|")
    for position, attribute in enumerate(fileHeader):
        if "START_ID" in attribute:
            schema.addEntry(position, RelationType.START_ID)
            sourceName = re.findall("\(([^)]+)\)", attribute)
            if len(sourceName) == 1:
                schema.setSourceName(sourceName[0] + "Id")
            else:
                schema.setSourceName("startId")

        elif "END_ID" in attribute:
            schema.addEntry(position, RelationType.END_ID)
            targetName = re.findall("\(([^)]+)\)", attribute)
            if len(targetName) == 1:
                schema.setTargetName(targetName[0] + "Id")
            else:
                schema.setSourceName("endId")

        else:
            schema.addEntry(position, RelationType.PROPERTY)
            # Get property name and optionally, type
            if ":" in attribute:
                propertyName = attribute.split(":")[0]
                propertyType = mapToSouffleType(attribute.split(":")[1])

            else:
                propertyName = attribute
                propertyType = "unsigned"  # default type set to integer

            schema.addProperty(position, propertyName, propertyType)

    return schema


def createRelation(inputFile: Any, relationSchema: RelationSchema) -> List[Relation]:
    relationList = []

    # Skip header
    _ = inputFile.readline()

    # Iterate rows
    for row in inputFile.readlines():
        rowData = row.strip("\n").split("|")
        relation = Relation()

        for position, value in enumerate(rowData):
            if relationSchema.getEntryType(position) == RelationType.START_ID:
                relation.setSourceId(value)

            elif relationSchema.getEntryType(position) == RelationType.END_ID:
                relation.setTargetId(value)

            elif relationSchema.getEntryType(position) == RelationType.PROPERTY:
                # Get property name
                propertyName = relationSchema.getPropertyName(position)
                propertyType = relationSchema.getPropertyTypeByPosition(position)

                # Handel Null case:
                if value == "":
                    if propertyType == "symbol":
                        value = "NULL"
                    elif propertyType == "unsigned":
                        value = 0
                    else:
                        raise Exception(
                            f"Warning: Unknown property type {propertyType} found in relation schema"
                        )

                relation.setProperty(propertyName, value)

            else:
                raise Exception("Error: Unknown Relation Type detected.")

        if relation.getLabel() == None:
            relation.setLabel(relationSchema.getGlobalLabel())

        relationList.append(relation)

    return relationList


# ----------- functions for writing relation declarations and facts ------#


def writeRelationDeclation(relationSchema: RelationSchema, outputFile: Any) -> None:
    """
    Write Datalog equivalent relation declaration to outputFile given a relation schema object
    """

    # Get relation label first
    globalLabel = relationSchema.getGlobalLabel()
    output = f".decl {globalLabel}("

    # Get start node id name
    sourceName = relationSchema.getSourceName()
    output += f"{sourceName}:unsigned, "

    # Get end node id name
    targetName = relationSchema.getTargetName()
    output += f"{targetName}:unsigned"

    # Get property (optional)
    propertyNameList = relationSchema.getPropertyNames()
    if len(propertyNameList) > 0:
        for propertyName in propertyNameList:
            propertyType = relationSchema.getPropertyTypeByName(propertyName)
            output += f", {propertyName}:{propertyType}"

    output += ")\n"
    output += f'.input {globalLabel}(IO=file, filename="{globalLabel}.facts")\n'

    outputFile.write(output)

    return


def writeRelationFacts(relation: Relation, outputFile: Any) -> None:
    """
    Write Datalog relation facts to outputFile given a list of relation objects
    """

    # Get source node identifier
    startId = relation.getSourceId()

    # Get target node identifier
    endId = relation.getTargetId()

    output = startId + "\t" + endId

    # Get property value (optional)
    propertyMapping = relation.getProperty()
    for _, value in propertyMapping.items():
        output += "\t" + value
    output += "\n"

    # Write
    outputFile.write(output)

    return

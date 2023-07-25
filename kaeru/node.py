"""
Module bundling all functions needed to extract Node schema and facts 
"""


from typing import Set, Mapping, List, Any
import re
from enum import Enum
import re 

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
        self.nodeGlobalLabel = None # group label, only matters for schema
        self.nodeProperty = {} # a map of tuple (key: position, value : (name, type)) 
        self.hasSubLabels = False
        self.subLabelsPosition = None

    def addEntry(self, position:int, fieldType:NodeType) -> None:
        self.entryToField[position] = fieldType
    
    def addProperty(self, position:int, propertyName:str, propertyType:str) -> None:
        self.nodeProperty[position] = (propertyName, propertyType)

    def addSubLabel(self, label:str) -> None:
        self.nodeSubLabels.add(label)

    def setGlobalLabel(self, label:str) -> None:
        self.nodeGlobalLabel = label

    def setSubLabelPosition(self, position:int) -> None:
        self.hasSubLabels = True
        self.subLabelsPosition = position
    
    def getEntryType(self, position:int) -> NodeType:
        if position < len(self.entryToField):
            return self.entryToField[position]
        else:
            raise Exception("Error: position exceeds the bound of schema")
        
    def getPropertyName(self, position:int) -> str:
        if position < len(self.entryToField):
            return self.nodeProperty[position][0]
        else:
            raise Exception("Error: position exceeds the bound of schema")
        
    def getNodeSubLabels(self) -> Set[str]:
        return self.nodeSubLabels
    
    def getNodeGlobalLabel(self) -> str:
        return self.nodeGlobalLabel
    
    def getPropertyNameAndType(self) -> Mapping:
        return self.nodeProperty



class Node:

    def __init__(self):
        self.id = None
        self.label = None # for printing to which file? 
        self.property = {} # a map of property name to its value 

    def setId(self, identifier:str) -> None:
        self.id = identifier

    def setLabel(self, label:str) -> None:
        self.label = label

    def setProperty(self, propertyName:str, propertyValue: str) -> None:
        self.property[propertyName] = propertyValue

    def getId(self) -> str:
        return self.id
    
    def getPropertyValue(self, propertyName) -> str : 
        return self.property[propertyName]
    
    def getPropertyNames(self) -> List[str] : 
        return list(self.property.keys())
    
    def getProperty(self) -> Mapping:
        return self.property
    
    def getLabel(self) -> str:
        return self.label



#--------- Helper functions ---------# 
def mapToSouffleType(propertyType):

    if propertyType == "STRING":
        return "symbol"
    elif propertyType == "LONG":
        return "unsigned"
    elif propertyType == "INT":
        return "unsigned"
    else:
        raise Exception(f"Unknown type {propertyType} found in Neo4j data file")


def createNodeSchema(inputFile:Any, label:str|None) -> NodeSchema:
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
            schema.setSubLabelPosition(position) # there are sub labels in the file, keep searching the file

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




    """ 
    Given a node, rename its property name to {label}{property} if 
    label is not found in the given propertyName.
    
    Why: to handle the following case: 
    where OrgName mismatch with sub label Company

    id:ID(Organisation)|:LABEL|OrgName:STRING|OrgUrl:STRING
    0|Company|Kam_Air|http://dbpedia.org/resource/Kam_Air
    1|Company|Balkh_Airlines|http://dbpedia.org/resource/Balkh_Airlines

    """


def createNodes(inputFile:Any, nodeSchema:NodeSchema) -> List[Node]:
    
    nodeList = []

    # skip header
    _ = inputFile.readline()

    # loop over rows in the data file 
    for row in inputFile.readlines():
        rowData = row.strip("\n").split(",")
        node = Node()
        
        for position, value in enumerate(rowData):
            
            if nodeSchema.getEntryType(position) == NodeType.ID:
                node.setId(value)

            elif nodeSchema.getEntryType(position) == NodeType.LABEL:
                node.setLabel(value)

            elif nodeSchema.getEntryType(position) == NodeType.PROPERTY:
                # get property name from schema
                propertyName = nodeSchema.getPropertyName(position)
                node.setProperty(propertyName, value)

            else:
                raise Exception("Error: Unknown Node Type detected.")
            
        nodeList.append(node)
    
    return nodeList


#------- Output Helper functions ------# 

def writeColumnBasedNodePropertyDeclHelper(nodeSchema:NodeSchema, outputFile:Any) -> None:
    # TODO: logic can be simplied a bit 
    nodeProperty = nodeSchema.getPropertyNameAndType()
    for _, valueTuple in nodeProperty.items():
        propertyName = valueTuple[0]
        propertyType = valueTuple[1]
        # property rename based on sub labels
        if nodeSchema.hasSubLabels:
            subLabelList = nodeSchema.getNodeSubLabels()
            for subLabel in subLabelList:
                # if the original property name obtained from input data file 
                # uses the global/parent label, rename to sublabel 
                if subLabel.lower() not in propertyName.lower():
                    # find the second capital letter position in propertyName 
                    s = re.search(r'^([^A-Z]*[A-Z]){2}', propertyName)
                    pos = s.span()[1]
                    newPropertyName = subLabel + propertyName[pos-1:]
                    
                    outputFile.write(f".decl {newPropertyName}(id:unsigned, {newPropertyName}:{propertyType})\n")
                    outputFile.write(f".input {newPropertyName}(IO=file, filename=\"{newPropertyName}.facts\")\n") 
                    outputFile.write("\n") 
                
                else:
                    raise Exception("Warning: input data file has strange property naming")
        else:
            outputFile.write(f".decl {propertyName}(id:unsigned, {propertyName}:{propertyType})\n")
            outputFile.write(f".input {propertyName}(IO=file, filename=\"{propertyName}.facts\")\n")  
            outputFile.write("\n")

    return 


def writeColumnBasedNodePropertyUnionDeclHelper(nodeSchema:NodeSchema, outputFile:Any) -> None:

    nodeProperty = nodeSchema.getPropertyNameAndType()
    for _, valueTuple in nodeProperty.items():
        # Get Property name part only 
        propertyName = valueTuple[0]
        propertyType = valueTuple[1]
        pos = re.search(r'^([^A-Z]*[A-Z]){2}', propertyName).span()[1] 
        propertyName = propertyName[pos-1:]
        # property rename based on sub labels
        if nodeSchema.hasSubLabels:
            globalLabel = nodeSchema.getNodeGlobalLabel()
            outputFile.write(f".decl {globalLabel}{propertyName}(id:unsigned, {propertyName}:{propertyType})\n")
            subLabelList = nodeSchema.getNodeSubLabels()
            for subLabel in subLabelList:
                # if the original property name obtained from input data file 
                # uses the global/parent label, rename to sublabel 
                    
                outputFile.write(f"{globalLabel}{propertyName}(id, {propertyName}) :- {subLabel}{propertyName}(id, {propertyName})\n")
        outputFile.write("\n") 
                
    return 


def writeColumnBasedNodeIdDeclHelper(nodeSchema:NodeSchema, outputFile:Any) -> None:

    # Node identifier 
    if nodeSchema.hasSubLabels:
        nodeLabelSets = nodeSchema.getNodeSubLabels()
    else:
        nodeLabelSets = set(nodeSchema.getNodeGlobalLabel())
    
    for label in nodeLabelSets:
        outputFile.write(f".decl {label}(id:unsigned)\n")
        outputFile.write(f".input {label}(IO=file, filename=\"{label}.facts\")\n")
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


#------- Output functions ------# 

def writeColumnBasedNodeDeclaration(nodeSchema:NodeSchema, outputFile:Any) -> None:
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


def writeRowBasedNodeDeclaration(nodeSchema:NodeSchema, outputFile:Any) -> None:
    """ 
    Generate row-based Datalog Node schema given Neo4j property graph 
    schema and write to a file at filePath
    
    Examples of row-based Datalog Node schema: 
    
    .decl City(id:number, cityname:symbol, score: number)
    """

    nodeLabelSets = nodeSchema.getNodeLabels()
    nodeProperty = nodeSchema.getPropertyNameAndType()
    for label in nodeLabelSets:
        output = f".decl {label}(id:unsigned"
        for _, valueTuple in nodeProperty.items():
            print(f"property : {valueTuple}")
            propertyName = valueTuple[0]
            propertyType = valueTuple[1]
            output += f", {propertyName}:{propertyType}"
        
        output += ")\n"
        
        outputFile.write(output)
        outputFile.write(f".input {label}(IO=file, filename=\"{label}.facts\")\n")
        outputFile.write("\n")

    return 


def writeColumnBasedNodeIdentifierFacts(node:Node, outputFile:Any) -> None:
    """ 
    Generate column-based Datalog Node facts given Neo4j property graph 
    data and write to a file at filePath.
    """

    identifier = node.getId()
    outputFile.write(identifier + "\n")

    return 


def writeColumnBasedNodePropertyFacts(node:Node, propertyName:str, outputFile:Any) -> None:
    """ 
    Generate column-based Datalog Node facts given Neo4j property graph 
    data and write to a file at filePath.
    """

    propertyValue = node.getPropertyValue(propertyName)
    identifier = node.getId()
    outputFile.write(identifier + "\t" + propertyValue +  "\n") 

    return 


def writeRowBasedNodeFacts(node:Node, nodeSchema:NodeSchema, outputFile:Any) -> None:
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


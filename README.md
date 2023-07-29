<div align="center">
<img src="./icon/kaeru_icon.png" 
alt="logo", width="60"></img>
</div>

<h1 style="text-align: center;">Kaeru</h1>
"Kaeru" is a Japanese word, pronounced as "Kah-eh-roo". When used as a verb, it can mean "translate" or "change" in English. Interestingly, when used as a noun term, it means "Frog", which explains the icon above.

## What's Kaeru

Kaeru is a command-line tool that takes a property graph data in csv format and generates its equivalent Datalog extensional database (EDB) declarations and facts. 

Kaeru currently supports property graph data that follows [Neo4j csv format](https://neo4j.com/developer/guide-import-csv/#_converting_data_values_with_load_csv), and Datalog EDB that follows [Souffle syntax](https://souffle-lang.github.io/program) and [data format](https://souffle-lang.github.io/simple).  


## How to install 

Kaeru can be built and installed locally. Clone this repo and go to the directory where this readme locates. Execute the following command, 

`python -m pip install .`


## Example 

TBD 



## Developer mode 

You can modify the source code and test the package behaviour using 

`python -m pip install --editable . `

## Issue 
- [ ] Remove tab "\t" in string data, since output file uses tab as delimiter.







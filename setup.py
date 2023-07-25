from setuptools import setup

setup(
    name="Kaeru",
    version="0.1.0",
    description="Command line tool to convert Cypher data into Datalog EDBs",
    author="Youning Xia",
    python_requires=">=3.11",
    install_requires=[],
    entry_points={
        "console_scripts": [
            "kaeru=kaeru.cli:main"
        ],
    },
    tests_require=["pytest"]
)
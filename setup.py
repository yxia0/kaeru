from setuptools import setup, find_packages

setup(
    name="Kaeru",
    version="0.1.0",
    description="Command line tool to convert Cypher data into Datalog EDBs",
    author="Youning Xia",
    python_requires=">=3.11",
    install_requires=[],
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    entry_points={
        "console_scripts": ["kaeru=kaeru.cli:main"],
    },
    tests_require=["pytest"],
)

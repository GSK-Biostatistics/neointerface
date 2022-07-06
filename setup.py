import setuptools
import os

with open("README.md", "r") as fh:
    long_description = fh.read()

def read_text(file_name: str):
    return open(os.path.join(file_name)).read()

setuptools.setup(
    name="neointerface",                     # This is the name of the package
    version="3.1.5",                         # The initial release version
    author="Alexey Kuznetsov, Julian West",  # Full name of the authors
    description="A Python interface to use the Neo4j graph database",
    long_description=long_description,      # Long description read from the the readme file
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(exclude=["tests"]),    # List of all python modules to be installed
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],                                      # Information to filter the project on PyPi website
    license=read_text("LICENSE"),
    python_requires='>=3.6',                # Minimum version requirement of the package
    # package_dir={'':''},                  # Directory of the source code of the package
    install_requires=["numpy==1.19.5", "pandas==1.1.5", "neo4j==4.4.0", "requests==2.25.1"]      # Install other dependencies if any
)
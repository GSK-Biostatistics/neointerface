import setuptools
import os

def read_text(file_name: str):
    return open(os.path.join(file_name)).read()


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

required = []
dependency_links = []

# Do not add to required lines pointing to Git repositories
EGG_MARK = '#egg='
for line in requirements:
    if line.startswith('-e git:') or line.startswith('-e git+') or \
            line.startswith('git:') or line.startswith('git+'):
        if EGG_MARK in line:
            package_name = line[line.find(EGG_MARK) + len(EGG_MARK):]
            required.append(package_name)
            dependency_links.append(line)
        else:
            print('Dependency to a git repository should have the format:')
            print('git+ssh://git@github.com/xxxxx/xxxxxx#egg=package_name')
    else:
        required.append(line)

setuptools.setup(
    name="neointerface",                     # This is the name of the package
    version="3.3.1",                         # The initial release version
    author="Alexey Kuznetsov, Julian West, Ben Grinsted, William McDermott",  # Full name of the authors
    description="A Python interface to use the Neo4j graph database",
    long_description="https://github.com/GSK-Biostatistics/neointerface/blob/main/README.md",      # Long description read from the the readme file
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(exclude=["tests"]),    # List of all python modules to be installed
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],                                      # Information to filter the project on PyPi website
    license=read_text("LICENSE"),
    python_requires='>=3.6',                # Minimum version requirement of the package
    # package_dir={'':''},                  # Directory of the source code of the package
    install_requires=required,
    dependency_links=dependency_links
)

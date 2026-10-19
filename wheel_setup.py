from setuptools import setup, find_packages
"""setuptool setup for the wheel package. find_packages() is used to automatically discover python
all packages and subpackages."""

setup(
    name = "dab_project",
    version = "0.1.0",
    description = "this contains the code in the ./src directory of the project",
    author = "Abhishek Kumar",
    packages = find_packages(where = "./src"),
    package_dir = {"": "./src"},
    install_requires = ["stetuptools"]
)
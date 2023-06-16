#!/usr/bin/env python

from distutils.core import setup

import toml
import versioneer
from setuptools import find_packages

pipfile = toml.load("Pipfile")
packages = [pckg for pckg in pipfile["packages"].keys()]
dev_packages = [pckg for pckg in pipfile["dev-packages"].keys()]

setup(
    # Package metadata
    name="prefecto",
    description="Prefect development aid.",
    author="Dominic Tarro",
    author_email="dtarro@oxfordeconomics.com",
    url="https://github.com/dominictarro/prefecto",
    project_urls={
        "Documentation": "https://dominictarro.github.io/prefecto/",
        "Source": "https://github.com/dominictarro/prefecto",
    },
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    # Versioning
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    # Package setup
    packages=find_packages(where="src", exclude=["pandas/*", "polars/*"]),
    package_dir={"": "src"},
    include_package_data=True,
    # Requirements
    python_requires=">=3.7",
    install_requires=packages,
    extras_require={"dev": dev_packages},
    classifiers=[
        "Natural Language :: English",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries",
    ],
)

#!/usr/bin/env python

from distutils.core import setup

import toml
import versioneer
from setuptools import find_packages


def pipenv_to_requirements(section: dict) -> list[str]:
    """Converts pipenv packages to requirements strings recognizable by `pip`.

    Example:
        ```python
        pipenv_to_requirements(
            {
                "pckg1": "*",
                "pckg2": {"version": ">=0.9.0", "extras": "extra1"},
                "pckg3": ">=1.2.1",
                "pckg4": {"version": "*", "extras": ["extra1", "extra2"]},
            }
        )
        ```
        ```
        ["pckg1", "pck2[extra1]>=0.9.0", "pckg3>=1.2.1", "pckg4[extra1,extra2]"]
        ```
    """
    requirements = []
    for k, v in section.items():
        if isinstance(v, dict):
            reqstring = k
            if "extras" in v:
                if len(v["extras"]) > 1:
                    reqstring += f"[{','.join(v['extras'])}]"
                else:
                    reqstring += f"[{v['extras'][0]}]"
            if "version" in v and v["version"] != "*":
                reqstring += f"{v['version']}"
        else:
            reqstring = k
            if v != "*":
                reqstring += f"{v}"
        requirements.append(reqstring)
    return requirements


pipfile = toml.load("Pipfile")

packages = pipenv_to_requirements(pipfile["packages"])
dev_packages = pipenv_to_requirements(pipfile["dev-packages"])
pandas_packages = pipenv_to_requirements(pipfile["pandas"])
polars_packages = pipenv_to_requirements(pipfile["polars"])

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
    extras_require={
        "dev": dev_packages,
        "pandas": pandas_packages,
        "polars": polars_packages,
    },
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

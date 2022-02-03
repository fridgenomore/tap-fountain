#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="tap-fountain",
    version="0.9.0",
    description="Singer.io tap for extracting date from fountain via API",
    author="FNM",
    url="https://github.com/fridgenomore/tap-fountain",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_fountain"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
        "attrs",
    ],
    entry_points="""
    [console_scripts]
    tap_fountain=tap_fountain:main
    """,
    packages=find_packages(),
    package_data = {
        "tap_fountain": ["schemas/*.json"]
    },
    include_package_data=True,
)
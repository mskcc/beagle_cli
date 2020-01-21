#!/usr/bin/env python
# -*- coding: utf-8 -*-
import setuptools

install_requires = [ i.strip() for i in open("requirements.txt").readlines() ]

setuptools.setup(
    name='beagle_cli',
    version='20.1.0',
    scripts=['beagle_cli'] ,
    description="Beagle API command line tool",
    url="https://github.com/mskcc/beagle_cli",
    packages=setuptools.find_packages(),
    install_requires = install_requires
)

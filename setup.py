# -*- coding: utf-8 -*-
"""
Created on Thu Jul 21 13:52:13 2022

@author: Wuestney

This setup file is based on the template accessed
at https://github.com/pypa/sampleproject on 7/21/2022.
"""

from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).resolve().parent

long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
      name = "pyusm",
      version = "0.1.0",
      description = "A Python implementation of Universal Sequence Maps.",
      long_description = long_description,
      long_description_content_type = "text/markdown",
      url = "https://github.com/katherine983/pyusm",
      author = "Katherine Wuestney",
      # author_email = "katherineann983@gmail.com",
      keywords = "CGR, chaos game representation, universal sequence maps, USM, iterated function systems",
      install_requires = ['nbib>=0.3',
                          'numpy>=1.20',
                          'pandas>=1.4',
                          'xlrd>=2.0',
                          'openpyxl>=3.0'
                          ],
      packages = ['pyusm'],
      python_requires = ">=3.8")
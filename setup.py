# coding: utf-8

import os
from io import open
import sys
import platform

from setuptools import setup, find_packages, Extension
from setuptools.command.build_ext import build_ext as _build_ext


class build_ext(_build_ext):
    def finalize_options(self):
        _build_ext.finalize_options(self)
        # Prevent numpy from thinking it is still in its setup process:
        if sys.version_info[0] >= 3:
            import builtins
            if hasattr(builtins, '__NUMPY_SETUP__'):
                del builtins.__NUMPY_SETUP__
            import importlib
            import numpy
            importlib.reload(numpy)
        else:
            import __builtin__
            if hasattr(__builtin__, '__NUMPY_SETUP__'):
                del __builtin__.__NUMPY_SETUP__
            import imp
            import numpy
            imp.reload(numpy)

        self.include_dirs.append(numpy.get_include())

SETUP_PTH = os.path.dirname(__file__)

extra_link_args = []
if sys.platform.startswith('win') and platform.machine().endswith('64'):
    extra_link_args.append('-Wl,--allow-multiple-definition')

with open(os.path.join(SETUP_PTH, "README.rst")) as f:
    long_desc = f.read()
    ind = long_desc.find("\n")
    long_desc = long_desc[ind + 1:]

setup(
    name="tornado",
    packages=find_packages(),
    version="1.0",
    cmdclass={'build_ext': build_ext},
    setup_requires=['numpy', 'setuptools>=18.0'],
    install_requires=["numpy>=1.9", "scipy>=0.14", "matplotlib>=1.5", "pyspark>=1.6"],
    extras_require={
        ':python_version == "2.7"': [
            'enum34',
        ],
        "matproj.snl": ["pybtex"],
        "pourbaix diagrams, bandstructure": ["pyhull>=1.5.3"],
        "ase_adaptor": ["ase>=3.3"],
        "vis": ["vtk>=6.0.0"],
        "abinit": ["pydispatcher>=2.0.5", "apscheduler==2.1.0"]},
    package_data={},
    author="Tornado Development Team",
    author_email="sadatnazrul@gmail.com",
    maintainer="Syed Sadat Nazrul",
    maintainer_email="sadatnazrul@gmail.com",
    url="https://www.youtube.com/c/PyRevolution",
    license=None,
    description="Tornado is a robust, Python and Spark based statistical" 
                 "library designed for Big Data applications.",
    long_description=long_desc,
    keywords=["Big Data", "Spark", "PySpark", "Statistics"],
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Data Scientists",
        "Operating System :: OS Independent",
        "Topic :: Statistics",
        "Topic :: Big Data",
        "Topic :: Spark",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
    ext_modules=[]
)

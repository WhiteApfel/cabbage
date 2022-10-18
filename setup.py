# -*- coding: utf-8 -*-
import os
from setuptools import setup


def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()


def get_version():
    """Get version from the package without actually importing it."""
    init = read("cabbage/__init__.py")
    for line in init.split("\n"):
        if line.startswith("__version__"):
            return eval(line.split("=")[1])


setup(
    name="cabbage",
    version=get_version(),
    description="asyncio-based AMQP client and server for RPC.",
    packages=["cabbage"],
    url="https://github.com/shocking-rodents/cabbage/",
    download_url="https://pypi.org/project/cabbage/",
    install_requires=read("requirements.txt"),
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    maintainer="Andrey Zakharov",
    maintainer_email="andrey.v.zakharov@yandex.ru",
    license="Apache License (2.0)",
    python_requires="~=3.7",
    zip_safe=True,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities",
    ],
)

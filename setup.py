import os
from setuptools import setup, find_packages


def read(fname):
    with open(fname) as f:
        return f.read()


def requirements():
    req = read('requirements.txt').replace('\r','').split('\n')
    return [r for r in req if r]


setup(
    name="pulsar-pusher",
    author="Luca Sbardella",
    author_email="luca@quantmmind.com",
    license="BSD",
    version='0.1.0',
    install_requires=requirements(),
    packages=find_packages(),
)

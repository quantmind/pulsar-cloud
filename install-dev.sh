#!/usr/bin/env bash

pip install --upgrade pip wheel
pip install --upgrade setuptools
pip install -r requirements-dev.txt
pip install -r requirements.txt
pip install git+https://github.com/quantmind/pulsar-agile.git

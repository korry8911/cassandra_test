#!/bin/bash

cd cassandra_test
pip install virtualenv
virtualenv venv
source ./venv/bin/activate
pip install -r ./requirements.txt

# Pull Cassandra images
docker pull cassandra:latest

# Run the tests
pytest test.py -s
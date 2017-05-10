# cassandra_test

### Testing Cassandra Materialized Views

#### Overview

This repo use `pytest`, `docker-machine`, `docker` to test the basic functionality (CRUD) of Cassandra materialized views. The tests were developed on OSX using Vagrant for the docker-machine. Each test will spin up a fresh, dockerized, 1 node Cassandra cluster (can be expanded later for multi-node clusters). The tests will then create a new Cassandra session, table and materialized view and execute commands to test creation, read, update, and delete functionality of the materialized view.

#### Setup and Running

To run the test you must have python, pip, docker, virtualbox, and docker-machine on OSX running. Please see [here](https://docs.docker.com/machine/install-machine/#installing-machine-directly) to setup docker-machine on OSX. Next, you will need to create a new docker-machine, clone this repo, install and activate virtualenv, install the requirements, then run the tests.

```
# Create new docker-machine
docker-machine create --driver virtualbox default
eval $(docker-machine env default --shell=bash)

# Setup environment
git clone https://github.com/korry8911/cassandra_test.git
cd cassandra_test
pip install virtualenv
source ./venv/bin/activate
pip install -r ./requirements.txt

# Run the tests
pytest test.py -s
```

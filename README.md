# cassandra_test

### Testing Cassandra Materialized Views

#### Overview

This repo use `pytest` and `docker` to test the basic functionality (CRUD) of Cassandra materialized views. The tests were developed on OSX using Vagrant for the docker-machine. Each test will spin up a fresh, dockerized, 1 node Cassandra cluster (can be expanded later for multi-node clusters). The tests will then create a new Cassandra session, table and materialized view and execute commands to test creation, read, update, and delete functionality of the materialized view.

#### Setup and Running

To run the test you must have python, pip, docker, virtualbox, and docker-machine on OSX running. Please see [here](https://docs.docker.com/machine/install-machine/#installing-machine-directly) to setup docker-machine on OSX. Next, you will need to create a new docker-machine, clone this repo, install and activate virtualenv, install the requirements, pull down the [Cassandra docker image](https://hub.docker.com/_/cassandra/), then run the tests. `run.sh` will handle all the setup and the running of the tests.

```
# Create new docker-machine
docker-machine create --driver virtualbox default
eval $(docker-machine env default --shell=bash)

# Clone and run
git clone https://github.com/korry8911/cassandra_test.git
sh ./cassandra_test/run.sh

# To run the tests again (don't re-run with run.sh)
pytest ./cassandra_test/test.py -s
```

### Outline of Test Cases

Here as some additional test cases that could be applied to testing Cassandra Materialized Views

#### Test Set 1: Does CRUD work (Basic Operation)

Implemented. This set of tests could be functional/blackbox. The tests would take in a set of basic functionality requirements and attempt to verify that the requirements are met or not.  The implementation of these tests in this repo checks if you can create a materialized view from a table (empty or not), read data from the materialized view that was in the table prior to creation of the materialized view and data that was written after the view was created, update data in the materialized view that was already in the base table and written after the creation of the materialized view, and finally if you can delete data from the materialized view that was already in the base table and written after the creation of the materialized view.

#### Test Set 2: Do upgrade/downgrades work, adding more nodes to cluster work (More complex operations)

The basic CRUD tests are not enough. Testing the materialized views should also take into consideration cluster configuration. These tests would verify if materialized views function properly after upgrading or downgrading the cluster, adding or removing nodes from the cluster, changing table structure or config, etc. These test would build upon the framework of the CRUD tests by exposing more control (i.e. let the tests add or remove nodes from the cluster) of the Cassandra cluster in the testing code.

#### Test Set 3: Are failures properly taken care of, (i.e. what happens if no ACK recieved from view store operation) (Failure Cases)

The previous tests mainly focus on meeting the requirements of functionality in a positive way. We also need to ensure proper behavior in the event of errors, failures, partitions, etc. These test would need to be more finely controlled as we need to be able to introduce failures reliably. There are many libraries for introducing network partitions that could be used to test the consistency of the materialized view in the event of a network partition or dropped packet. 

#### Test Set 4: Cross-platform testing, does it work on OSX, Ubunutu, Debian, Centos, etc

Simple, these test could just modify the current tests so they can run on multiple platforms and against Cassandra running on different platforms

#### Test Set 5: Performance (operational performance)

Also straightforward. What are the performance characteristics of the materialized view operations. How long does it take for a write/read/update/delete to complete and show up in the materialized view? How much does I/O load affect materialized view latency compared to the base table? How much CPU does the materialzied view use for differing levels of IO load? Questions like these can be answered by monitoring system resource usage and utilization during different load types and sizes, cluster types and sizes, etc.
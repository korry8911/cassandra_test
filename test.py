import pytest
import docker
import time
import uuid
import random, string
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, tuple_factory
from cassandra import ConsistencyLevel

### Helper functions

def random_keyspace():
    return ''.join(random.choice(string.lowercase) for i in range(10))

def random_user():
    return ''.join(random.choice(string.lowercase) for i in range(10))

def create_keyspace_cmd(keyspace):
    string = """
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % keyspace
    return string

def create_table_cmd():
    string = """
        CREATE TABLE scores
        (
        user TEXT,
        game TEXT,
        year INT,
        month INT,
        day INT,
        score INT,
        PRIMARY KEY (user, game, year, month, day)
        )
        """
    return string

def create_materialized_view_cmd():
    string = """
        CREATE MATERIALIZED VIEW alltimehigh AS
        SELECT user FROM scores
        WHERE game IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL
        PRIMARY KEY (game, score, user, year, month, day)
        WITH CLUSTERING ORDER BY (score desc)
        """
    return string

def all_views(session):
    return list(session.execute("""SELECT * FROM system_schema.views"""))

def all_base_data(session):
    return list(session.execute("""SELECT * FROM scores"""))

def all_view_data(session):
    return list(session.execute("""SELECT * FROM alltimehigh"""))

def insert_data_base_table(session, size):
    insert_data = session.prepare("INSERT INTO scores (user, game, year, month, day, score) VALUES (?, ?, ?, ?, ?, ?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
    data = list()
    for i in range(0,size):
        data_point = (random_user(), random.choice(['tennis', 'golf']),\
                                        random.choice([2016, 2017]),\
                                        random.choice(range(1,12)),\
                                        random.choice(range(1,30)), i)
        data.append(data_point)
        batch.add(insert_data, data_point)
    session.execute(batch)
    time.sleep(2)
    return data

def delete_data_base_table(session, data_to_delete):
    delete_data = session.prepare("DELETE FROM scores WHERE user=? AND game=? AND year=? AND month=? AND day=?")
    batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
    for item in data_to_delete:
        batch.add(delete_data, item[0:-1])
    session.execute(batch)
    time.sleep(2)
    return

def insert_updated_data_base_table(session, data_to_update):
    insert_data = session.prepare("INSERT INTO scores (user, game, year, month, day, score) VALUES (?, ?, ?, ?, ?, ?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
    data = list()
    for item in data_to_update:
        data_point = list(item)
        data_point[5] = data_point[5]*2
        data.append(tuple(data_point))
        batch.add(insert_data, data_point)
    session.execute(batch)
    time.sleep(2)
    return data

class CassandraCluster:
    def __init__(self):
        self.cassandra_host = '192.168.99.100'
        self.docker_client = docker.from_env()

    def create_cluster(self):
        self.docker_client.containers.run("cassandra:latest", network_mode='host', detach=True)
        return

    def destroy_cluster(self):
        for container in self.docker_client.containers.list():
            if container.attrs['Config']['Image'] == "cassandra:latest":
                container.remove(force=True)
        return

    def wait_until_connected(self):
        connected = False
        while not connected:
            try:
                Cluster([self.cassandra_host]).connect()
                break
            except:
                print('Waiting for Cassandra connection...')
                time.sleep(3)
        return

    def session(self):
        return Cluster([self.cassandra_host]).connect()

@pytest.fixture
def cassandra_cluster(request):
    cluster = CassandraCluster()
    cluster.destroy_cluster()
    cluster.create_cluster()
    cluster.wait_until_connected()
    request.addfinalizer(cluster.destroy_cluster)
    return cluster
### Tests

@pytest.mark.timeout(15)
@pytest.mark.parametrize("cluster_config", [{'nodes': 1}])
@pytest.mark.parametrize("initial_table_data_size", [0,10])
def test_materialized_views_create_from_table(cassandra_cluster, cluster_config, initial_table_data_size):
    print('Testing Cassandra Materialized Views: creation from table with ' + str(initial_table_data_size)+ ' initial entries')
    print('Cluster config: ' + str(cluster_config))
    session = cassandra_cluster.session()
    #session = setup_cluster(cluster_config)

    # Create keyspace and table
    keyspace = random_keyspace()
    session.execute(create_keyspace_cmd(keyspace))
    session.set_keyspace(keyspace)
    session.execute(create_table_cmd())

    # Assert no data, no views for base table (bt)
    assert len(all_base_data(session)) == 0
    assert len(all_views(session)) == 0 

    data = insert_data_base_table(session, initial_table_data_size)
    assert len(all_base_data(session)) == initial_table_data_size
    assert len(all_views(session)) == 0

    # Create view from base table
    session.execute(create_materialized_view_cmd())
    time.sleep(2)

    # Assert view created from base table with initial data
    assert len(all_views(session)) == 1
    assert all_views(session)[0][1] == 'alltimehigh'
    assert all_views(session)[0][3] == 'scores'
    assert len(all_base_data(session)) == initial_table_data_size
    assert len(all_view_data(session)) == initial_table_data_size
    return


@pytest.mark.timeout(15)
@pytest.mark.parametrize("cluster_config", [{'nodes': 1}])
@pytest.mark.parametrize("initial_table_data_size", [0,10])
def test_materialized_views_create_from_table(cassandra_cluster, cluster_config, initial_table_data_size):
    print('Testing Cassandra Materialized Views: creation from table with ' + str(initial_table_data_size)+ ' initial entries')
    print('Cluster config: ' + str(cluster_config))
    session = cassandra_cluster.session()

    # Create keyspace and table
    keyspace = random_keyspace()
    session.execute(create_keyspace_cmd(keyspace))
    session.set_keyspace(keyspace)
    session.execute(create_table_cmd())

    # Assert no data, no views for base table (bt)
    assert len(all_base_data(session)) == 0
    assert len(all_views(session)) == 0 

    data = insert_data_base_table(session, initial_table_data_size)
    assert len(all_base_data(session)) == initial_table_data_size
    assert len(all_views(session)) == 0

    # Create view from base table
    session.execute(create_materialized_view_cmd())
    time.sleep(2)

    # Assert view created from base table with initial data
    assert len(all_views(session)) == 1
    assert all_views(session)[0][1] == 'alltimehigh'
    assert all_views(session)[0][3] == 'scores'
    assert len(all_base_data(session)) == initial_table_data_size
    assert len(all_view_data(session)) == initial_table_data_size
    return

@pytest.mark.timeout(15)
@pytest.mark.parametrize("cluster_config", [{'nodes': 1}])
@pytest.mark.parametrize("initial_table_data_size", [0, 10])
@pytest.mark.parametrize("additional_data_size", [0, 10])
def test_materialized_views_read_from_view(cassandra_cluster, cluster_config, initial_table_data_size, additional_data_size):
    print('Testing Cassandra Materialized Views: read from view with ' + str(initial_table_data_size)\
             + ' initial entries and ' + str(additional_data_size) + ' additional entries')
    print('Cluster config: ' + str(cluster_config))
    session = cassandra_cluster.session()

    # Create keyspace and table
    keyspace = random_keyspace()
    session.execute(create_keyspace_cmd(keyspace))
    session.set_keyspace(keyspace)
    session.execute(create_table_cmd())

    # Assert no data, no views for base table (bt)
    assert len(all_views(session)) == 0 
    assert len(all_base_data(session)) == 0

    data_initial = insert_data_base_table(session, initial_table_data_size)
    assert len(all_base_data(session)) == initial_table_data_size
    assert len(all_views(session)) == 0 

    # Create view from base table
    session.execute(create_materialized_view_cmd())
    time.sleep(2)

    # Assert view contains initial data in base table
    assert all_base_data(session).sort(key=lambda x: x[5]) == data_initial.sort(key=lambda x: x[5])
    assert all_view_data(session).sort(key=lambda x: x[5]) == data_initial.sort(key=lambda x: x[5])

    data_additional = insert_data_base_table(session, additional_data_size)
    data_total = data_initial + data_additional

    # Assert view contains additional data in base table
    assert all_base_data(session).sort(key=lambda x: x[5]) == data_total.sort(key=lambda x: x[5])
    assert all_view_data(session).sort(key=lambda x: x[5]) == data_total.sort(key=lambda x: x[5])
    return

@pytest.mark.timeout(15)
@pytest.mark.parametrize("cluster_config", [{'nodes': 1}])
@pytest.mark.parametrize("initial_table_data_size", [0, 10])
@pytest.mark.parametrize("additional_data_size", [10])
def test_materialized_views_update_view_data(cassandra_cluster, cluster_config, initial_table_data_size, additional_data_size):
    print('Testing Cassandra Materialized Views: update data in view with ' + str(initial_table_data_size)\
             + ' initial entries and ' + str(additional_data_size) + ' additional entries')
    print('Cluster config: ' + str(cluster_config))
    session = cassandra_cluster.session()

    # Create keyspace and table
    keyspace = random_keyspace()
    session.execute(create_keyspace_cmd(keyspace))
    session.set_keyspace(keyspace)
    session.execute(create_table_cmd())

    # Assert no data, no views for base table (bt)
    assert len(all_views(session)) == 0 
    assert len(all_base_data(session)) == 0

    # Insert initial data
    data_initial = insert_data_base_table(session, initial_table_data_size)

    # Assert intial data is in base table
    assert len(all_base_data(session)) == initial_table_data_size
    assert len(all_views(session)) == 0 

    # Create view from base table
    session.execute(create_materialized_view_cmd())
    time.sleep(2)

    # Update initial data
    updated_intial_data = insert_updated_data_base_table(session, data_initial)

    # Assert view data from base table with updated initial data
    assert all_base_data(session).sort(key=lambda x: x[5]) == updated_intial_data.sort(key=lambda x: x[5])
    assert all_view_data(session).sort(key=lambda x: x[5]) == updated_intial_data.sort(key=lambda x: x[5])

    # Insert and update new data
    data_additional = insert_data_base_table(session, additional_data_size)
    updated_additional_data = insert_updated_data_base_table(session, data_additional)
    updated_total_data = updated_intial_data + updated_additional_data

    # Assert new data was updated
    assert all_base_data(session).sort(key=lambda x: x[5]) == updated_total_data.sort(key=lambda x: x[5])
    assert all_view_data(session).sort(key=lambda x: x[5]) == updated_total_data.sort(key=lambda x: x[5])

    # Update all data
    more_updated_total_data = insert_updated_data_base_table(session, updated_total_data)

    # Assert all data was updated
    assert all_base_data(session).sort(key=lambda x: x[5]) == more_updated_total_data.sort(key=lambda x: x[5])
    assert all_view_data(session).sort(key=lambda x: x[5]) == more_updated_total_data.sort(key=lambda x: x[5])
    return

@pytest.mark.timeout(15)
@pytest.mark.parametrize("cluster_config", [{'nodes': 1}])
@pytest.mark.parametrize("initial_table_data_size", [0, 10])
@pytest.mark.parametrize("additional_data_size", [10])
def test_materialized_views_delete_view_data(cassandra_cluster, cluster_config, initial_table_data_size, additional_data_size):
    print('Testing Cassandra Materialized Views: delete data in view with ' + str(initial_table_data_size)\
             + ' initial entries and ' + str(additional_data_size) + ' additional entries')
    print('Cluster config: ' + str(cluster_config))
    session = cassandra_cluster.session()

    # Create keyspace and table
    keyspace = random_keyspace()
    session.execute(create_keyspace_cmd(keyspace))
    session.set_keyspace(keyspace)
    session.execute(create_table_cmd())

    # Assert no data, no views for base table (bt)
    assert len(all_views(session)) == 0 
    assert len(all_base_data(session)) == 0

    # Insert initial data
    data_initial = insert_data_base_table(session, initial_table_data_size)

    # Assert intial data is in base table
    assert len(all_base_data(session)) == initial_table_data_size
    assert len(all_views(session)) == 0 

    # Create view from base table
    session.execute(create_materialized_view_cmd())
    time.sleep(2)

    #Add more data to table
    data_additional = insert_data_base_table(session, additional_data_size)
    data_total = data_initial + data_additional

    # Assert total data is in base table
    assert len(all_base_data(session)) == initial_table_data_size + additional_data_size
    assert len(all_view_data(session)) == initial_table_data_size + additional_data_size


    delete_data_base_table(session, data_total)
    # Assert view contains no data
    assert all_base_data(session) == []
    assert all_view_data(session) == []
    return

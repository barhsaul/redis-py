import pytest
from unittest.mock import call, patch, MagicMock, DEFAULT
from redis import Redis
from redis.cluster import ClusterNode, RedisCluster, \
    NodesManager, PRIMARY, REPLICA
from redis.connection import Connection
from redis.utils import str_if_bytes
from redis.exceptions import (
    AskError,
    ClusterDownError,
    MovedError,
    RedisClusterException,
)
from .conftest import skip_if_not_cluster_mode, _get_client

default_host = "127.0.0.1"
default_port = 7000


def get_mocked_redis_client(*args, **kwargs):
    """
    Return a stable RedisCluster object that have deterministic
    nodes and slots setup to remove the problem of different IP addresses
    on different installations and machines.
    """

    with patch.object(Redis, 'execute_command') as execute_command_mock:
        def execute_command(*_args, **_kwargs):
            if _args[0] == 'CLUSTER SLOTS':
                mock_cluster_slots = [
                    [
                        0, 8191,
                        ['127.0.0.1', 7000, 'node_0'],
                        ['127.0.0.1', 7003, 'node_3'],
                    ],
                    [
                        8192, 16383,
                        ['127.0.0.1', 7001, 'node_1'],
                        ['127.0.0.1', 7002, 'node_2']
                    ]
                ]
                return mock_cluster_slots
            elif _args[1] == 'cluster-require-full-coverage':
                return {'cluster-require-full-coverage': 'yes'}

        execute_command_mock.side_effect = execute_command

        return RedisCluster(*args, **kwargs)


def find_node_ip_based_on_port(cluster_client, port):
    for node in cluster_client.get_all_nodes():
        if node.port == port:
            return node.host


def moved_redirection_helper(request, failover=False):
    """
        Test that the client handles MOVED response after a failover.
        Redirection after a failover means that the redirection address is of a
        replica that was promoted to a primary.

        At first call it should return a MOVED ResponseError that will point
        the client to the next server it should talk to.

        Verify that:
        1. it tries to talk to the redirected node
        2. it updates the slot's primary to the redirected node

        For a failover, also verify:
        3. the redirected node's server type updated to 'primary'
        4. the server type of the previous slot owner updated to 'replica'
        """
    r = _get_client(RedisCluster, request, flushdb=False)
    slot = 12182
    redirect_node = None
    # Get the current primary that holds this slot
    prev_primary = r.nodes_manager.get_node_from_slot(slot)
    if failover:
        if len(r.nodes_manager.slots_cache[slot]) < 2:
            raise RedisClusterException("This test requires to have a replica")
        redirect_node = r.nodes_manager.slots_cache[slot][1]
    else:
        # Use one of the primaries to be the redirected node
        redirect_node = r.get_all_primaries()[0]
    r_host = redirect_node.host
    r_port = redirect_node.port
    with patch.object(Redis, 'parse_response') as parse_response:
        def moved_redirect_effect(connection, *args, **options):
            def ok_response(connection, *args, **options):
                assert connection.host == r_host
                assert connection.port == r_port

                return "MOCK_OK"

            parse_response.side_effect = ok_response
            raise MovedError("{0} {1}:{2}".format(slot, r_host, r_port))

        parse_response.side_effect = moved_redirect_effect
        assert r.execute_command("SET", "foo", "bar") == "MOCK_OK"
        slot_primary = r.nodes_manager.slots_cache[slot][0]
        assert slot_primary == redirect_node
        if failover:
            assert r.get_node(host=r_host, port=r_port).server_type == PRIMARY
            assert prev_primary.server_type == REPLICA


@skip_if_not_cluster_mode()
class TestRedisClusterObj:
    def test_host_port_startup_node(self):
        """
        Test that it is possible to use host & port arguments as startup node
        args
        """
        cluster = get_mocked_redis_client(host=default_host, port=default_port)
        assert cluster.get_node(host=default_host,
                                port=default_port) is not None

    def test_startup_nodes(self):
        """
        Test that it is possible to use startup_nodes
        argument to init the cluster
        """
        port_1 = 7000
        port_2 = 7001
        startup_nodes = [ClusterNode(default_host, port_1),
                         ClusterNode(default_host, port_2)]
        cluster = get_mocked_redis_client(startup_nodes=startup_nodes)
        assert cluster.get_node(host=default_host, port=port_1) is not None \
               and cluster.get_node(host=default_host, port=port_2) is not None

    def test_empty_startup_nodes(self):
        """
        Test that exception is raised when empty providing empty startup_nodes
        """
        with pytest.raises(RedisClusterException) as ex:
            RedisCluster(startup_nodes=[])

        assert str(ex.value).startswith(
            "RedisCluster requires at least one node to discover the "
            "cluster"), str_if_bytes(ex.value)

    def test_from_url(self, r):
        redis_url = "redis://{0}:{1}/0".format(default_host, default_port)
        with patch.object(RedisCluster, 'from_url') as from_url:
            def from_url_mocked(_url, **_kwargs):
                return get_mocked_redis_client(url=_url, **_kwargs)

            from_url.side_effect = from_url_mocked
            cluster = RedisCluster.from_url(redis_url)
        assert cluster.get_node(host=default_host,
                                port=default_port) is not None

    def test_skip_full_coverage_check(self):
        """
        Test if the cluster_require_full_coverage NodeManager method was not
         called with the flag activated
        """
        cluster = get_mocked_redis_client(default_host, default_port,
                                          skip_full_coverage_check=True)
        cluster.nodes_manager.cluster_require_full_coverage = MagicMock()
        assert not cluster.nodes_manager.cluster_require_full_coverage.called

    def test_execute_command_errors(self, r):
        """
        If no command is given to `_determine_nodes` then exception
        should be raised.

        Test that if no key is provided then exception should be raised.
        """
        with pytest.raises(RedisClusterException) as ex:
            r.execute_command()
        assert str(ex.value).startswith("Unable to determine command to use")

        with pytest.raises(RedisClusterException) as ex:
            r.execute_command("GET")
        assert str(ex.value).startswith("No way to dispatch this command to "
                                        "Redis Cluster. Missing key.")

    def test_ask_redirection(self, r):
        """
        Test that the server handles ASK response.

        At first call it should return a ASK ResponseError that will point
        the client to the next server it should talk to.

        Important thing to verify is that it tries to talk to the second node.
        """
        redirect_node = r.get_all_nodes()[0]
        with patch.object(Redis, 'parse_response') as parse_response:
            def ask_redirect_effect(connection, *args, **options):
                def ok_response(connection, *args, **options):
                    assert connection.host == redirect_node.host
                    assert connection.port == redirect_node.port

                    return "MOCK_OK"

                parse_response.side_effect = ok_response
                raise AskError("12182 {0}:{1}".format(redirect_node.host,
                                                      redirect_node.port))

            parse_response.side_effect = ask_redirect_effect

            assert r.execute_command("SET", "foo", "bar") == "MOCK_OK"

    def test_moved_redirection(self, request):
        """
        Test that the client handles MOVED response.
        """
        moved_redirection_helper(request, failover=False)

    def test_moved_redirection_after_failover(self, request):
        """
        Test that the client handles MOVED response after a failover.
        """
        moved_redirection_helper(request, failover=True)

    def test_refresh_using_specific_nodes(self, request):
        """
        Test making calls on specific nodes when the cluster has failed over to
        another node
        """
        node_7006 = ClusterNode(host=default_host, port=7006,
                                server_type=PRIMARY)
        node_7007 = ClusterNode(host=default_host, port=7007,
                                server_type=PRIMARY)
        with patch.object(Redis, 'parse_response') as parse_response:
            with patch.object(NodesManager, 'initialize', autospec=True) as \
                    initialize:
                with patch.multiple(Connection,
                                    send_command=DEFAULT,
                                    connect=DEFAULT,
                                    can_read=DEFAULT) as mocks:

                    # simulate 7006 as a failed node
                    def parse_response_mock(connection, command_name,
                                            **options):
                        if connection.port == 7006:
                            parse_response.failed_calls += 1
                            raise ClusterDownError(
                                'CLUSTERDOWN The cluster is '
                                'down. Use CLUSTER INFO for '
                                'more information')
                        elif connection.port == 7007:
                            parse_response.successful_calls += 1

                    def initialize_mock(self):
                        # start with all slots mapped to 7006
                        self.nodes_cache = {node_7006.name: node_7006}
                        self.slots_cache = {}

                        for i in range(0, 16383):
                            self.slots_cache[i] = [node_7006]

                        # After the first connection fails, a reinitialize
                        # should follow the cluster to 7007
                        def map_7007(self):
                            self.nodes_cache = {
                                node_7007.name: node_7007}
                            self.slots_cache = {}

                            for i in range(0, 16383):
                                self.slots_cache[i] = [node_7007]

                        # Change initialize side effect for the second call
                        initialize.side_effect = map_7007

                    parse_response.side_effect = parse_response_mock
                    parse_response.successful_calls = 0
                    parse_response.failed_calls = 0
                    initialize.side_effect = initialize_mock
                    mocks['can_read'].return_value = False
                    mocks['send_command'].return_value = "MOCK_OK"
                    mocks['connect'].return_value = None

                    rc = _get_client(
                        RedisCluster, request, flushdb=False)
                    assert len(rc.get_all_nodes()) == 1
                    assert rc.get_node(node_7006.name) is not None

                    rc.get('foo')

                    # Cluster should now point to 7007, and there should be
                    # one failed and one successful call
                    assert len(rc.get_all_nodes()) == 1
                    assert rc.get_node(node_7007.name) is not None
                    assert rc.get_node(node_7006.name) is None
                    assert parse_response.failed_calls == 1
                    assert parse_response.successful_calls == 1

    def test_reading_from_replicas_in_round_robin(self):
        with patch.multiple(Connection, send_command=DEFAULT,
                            read_response=DEFAULT, _connect=DEFAULT,
                            can_read=DEFAULT, on_connect=DEFAULT) as mocks:
            with patch.object(Redis, 'parse_response') as parse_response:
                def parse_response_mock_first(connection, *args, **options):
                    # Primary
                    assert connection.port == 7001
                    parse_response.side_effect = parse_response_mock_second
                    return "MOCK_OK"

                def parse_response_mock_second(connection, *args, **options):
                    # Replica
                    assert connection.port == 7002
                    parse_response.side_effect = parse_response_mock_third
                    return "MOCK_OK"

                def parse_response_mock_third(connection, *args, **options):
                    # Primary
                    assert connection.port == 7001
                    return "MOCK_OK"

                # We don't need to create a real cluster connection but we
                # do want RedisCluster.on_connect function to get called,
                # so we'll mock some of the Connection's functions to allow it
                parse_response.side_effect = parse_response_mock_first
                mocks['send_command'].return_value = True
                mocks['read_response'].return_value = "OK"
                mocks['_connect'].return_value = True
                mocks['can_read'].return_value = False
                mocks['on_connect'].return_value = True

                # Create a cluster with reading from replications
                read_cluster = get_mocked_redis_client(host=default_host,
                                                       port=default_port,
                                                       read_from_replicas=True)
                assert read_cluster.read_from_replicas is True
                # Check that we read from the slot's nodes in a round robin
                # matter.
                # 'foo' belongs to slot 12182 and the slot's nodes are:
                # [(127.0.0.1,7001,primary), (127.0.0.1,7002,replica)]
                read_cluster.get("foo")
                read_cluster.get("foo")
                read_cluster.get("foo")
                mocks['send_command'].assert_has_calls([call('READONLY')])


@skip_if_not_cluster_mode()
class TestClusterRedisCommands:
    def test_get_and_set(self, r):
        # get and set can't be tested independently of each other
        assert r.get('a') is None
        byte_string = b'value'
        integer = 5
        unicode_string = chr(3456) + 'abcd' + chr(3421)
        assert r.set('byte_string', byte_string)
        assert r.set('integer', 5)
        assert r.set('unicode_string', unicode_string)
        assert r.get('byte_string') == byte_string
        assert r.get('integer') == str(integer).encode()
        assert r.get('unicode_string').decode('utf-8') == unicode_string

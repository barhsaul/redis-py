import random
import warnings
import time
import socket
from redis.client import Redis
from redis.connection import (ClusterConnection, SSLClusterConnection, Encoder)
from redis.commands import (
    ClusterCommands, DataAccessCommands
)
from redis.exceptions import (
    ResponseError,
    TimeoutError,
    AskError,
    ClusterDownError,
    ClusterError,
    MovedError,
    RedisClusterException,
    SlotNotCoveredError,
    TryAgainError,
    BusyLoadingError,
)
from redis.utils import str_if_bytes, dict_merge, list_keys_to_dict
from redis.crc import crc16


def get_node_name(host, port):
    return f'{host}:{port}'


def get_connection(redis_node, *args, **options):
    return redis_node.connection or redis_node.connection_pool.get_connection(args[0], **options)


PRIMARY = 'master'
REPLICA = 'slave'
ALL_PRIMARIES = 'all-masters'
ALL_NODES = 'all-nodes'
RANDOM = 'random'
SLOT_ID = 'slot-id'

REDIS_ALLOWED_KEYS = (
    'host',
    'port',
    'db',
    'username',
    'password',
    'socket_timeout',
    'socket_connect_timeout',
    'socket_keepalive',
    'socket_keepalive_options',
    'connection_class',
    'connection_pool',
    'unix_socket_path',
    'encoding',
    'encoding_errors',
    'charset',
    'errors',
    'decode_responses',
    'retry_on_timeout',
    'ssl',
    'ssl_keyfile',
    'ssl_certfile',
    'ssl_cert_reqs',
    'ssl_ca_certs',
    'max_connections',
)
KWARGS_DISABLED_KEYS = (
    'host',
    'port',
    'decode_responses',
)

REDIS_CLUSTER_HASH_SLOTS = 16384

class RedisCluster(ClusterCommands, DataAccessCommands, object):
    RedisClusterRequestTTL = 16
    NODES_FLAGS = dict_merge(
        list_keys_to_dict([
            "CONFIG SET",
            "CLIENT LIST",
            "TIME",
        ], ALL_NODES),
        list_keys_to_dict([
            "SCAN",
            "KEYS",
        ], ALL_PRIMARIES),
        list_keys_to_dict([
            "CLUSTER NODES",
            "CLUSTER SLOTS",
            "CLUSTER REPLICAS"
            "RANDOMKEY",
        ], RANDOM),
        list_keys_to_dict([
            "CLUSTER COUNTKEYSINSLOT",
            "CLUSTER GETKEYSINSLOT",
            "CLUSTER SETSLOT",
        ], SLOT_ID),
    )

    def __init__(self, host=None, port=7000, startup_nodes=None, cluster_down_retry_attempts=3,
                 skip_full_coverage_check=False, reinitialize_steps=25, **kwargs):
        """
        :startup_nodes:
            List of nodes that initial bootstrapping can be done from
        :host:
            Can be used to point to a startup node
        :port:
            Can be used to point to a startup node
                :skip_full_coverage_check:
            Skips the check of cluster-require-full-coverage config, useful for clusters
            without the CONFIG command (like aws)
        :**kwargs:
            Extra arguments that will be sent into Redis instance when created
            (See Official redis-py doc for supported kwargs
            [https://github.com/andymccurdy/redis-py/blob/master/redis/client.py])
            Some kwargs is not supported and will raise RedisClusterException
            - db (Redis do not support database SELECT in cluster mode)
        """
        if "db" in kwargs:
            # Argument 'db' is not possible to use in cluster mode. Ignoring it
            raise RedisClusterException("Argument 'db' is not possible to use in cluster mode")
        if not startup_nodes:
            startup_nodes = []
        if host and port:
            startup_nodes.append(ClusterNode(host, port))

        self.reinitialize_counter = 0
        self.reinitialize_steps = reinitialize_steps
        self.refresh_table_asap = False
        self.reset_connections = False
        self.cluster_down_retry_attempts = cluster_down_retry_attempts
        self.nodes_flags = self.__class__.NODES_FLAGS.copy()
        self.encoder = Encoder(
            kwargs.get('encoding', 'utf-8'),
            kwargs.get('encoding_errors', 'strict'),
            kwargs.get('decode_responses', False)
        )
        # Currently we do not support passing connection classes,
        # we use ClusterConnection by default or SSLClusterConnection for SSL
        if kwargs.pop('ssl', False):
            connection_class = SSLClusterConnection
        else:
            connection_class = ClusterConnection
        kwargs.update({'connection_class': connection_class})
        kwargs = self._cleanup_kwargs(**kwargs)
        self.nodes_manager = NodesManager(startup_nodes=startup_nodes,
                                          skip_full_coverage_check=skip_full_coverage_check,
                                          **kwargs)

    def _cleanup_kwargs(self, **kwargs):
        """
        Remove unsupported or disabled keys from kwargs
        """
        connection_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k in REDIS_ALLOWED_KEYS and k not in KWARGS_DISABLED_KEYS
        }

        return connection_kwargs

    def get_redis_connection(self, node):
        if not node.redis_connection:
            self.nodes_manager.create_redis_connections([node])
        return node.redis_connection

    def get_node(self, node_name=None, host=None, port=None):
        if node_name is None:
            if not host or not port:
                warnings.warn("get_node requires one of the followings: 1. node name 2. host and port")
                return None
            if host == 'localhost':
                host = socket.gethostbyname(host)
            node_name = get_node_name(host=host, port=port)
        return self.nodes_manager.nodes_cache[node_name]

    def get_all_primaries(self):
        return self.nodes_manager.get_nodes_by_server_type(PRIMARY)

    def get_all_replicas(self):
        return self.nodes_manager.get_nodes_by_server_type(REPLICA)

    def get_random_node(self):
        """
        """
        return random.choice(list(self.nodes_manager.nodes_cache.values()))

    def get_all_nodes(self):
        return list(self.nodes_manager.nodes_cache.values())

    def _determine_nodes(self, *args, **kwargs):
        """
        """
        command = args[0]
        node_flag = self.nodes_flags.get(command)

        if node_flag == RANDOM:
            return [self.get_random_node()]
        elif node_flag == ALL_PRIMARIES:
            return self.get_all_primaries()
        elif node_flag == ALL_NODES:
            return self.get_all_nodes()
        else:
            # get the node that holds the key's slot
            slot = self.determine_slot(*args)
            return [self.nodes_manager.get_node_from_slot(slot)]

    def _increment_reinitialize_counter(self, count=1):
        # In order not to reinitialize the cluster, the user can set reinitialize_steps to 0.
        for i in range(min(1, self.reinitialize_steps)):
            self.reinitialize_counter += count
            if self.reinitialize_counter % self.reinitialize_steps == 0:
                self.initialize()

    def _should_reinitialized(self):
        # In order not to reinitialize the cluster, the user can set reinitialize_steps to 0.
        if self.reinitialize_steps == 0:
            return False
        else:
            return self.reinitialize_counter % self.reinitialize_steps == 0

    def keyslot(self, key):
        """
        Calculate keyslot for a given key.
        Tuned for compatibility with python 2.7.x
        """
        k = self.encoder.encode(key)

        start = k.find(b"{")

        if start > -1:
            end = k.find(b"}", start + 1)
            if end > -1 and end != start + 1:
                k = k[start + 1:end]

        return crc16(k) % REDIS_CLUSTER_HASH_SLOTS

    def determine_slot(self, *args):
        """
        figure out what slot based on command and args
        """
        if len(args) <= 1:
            raise RedisClusterException("No way to dispatch this command to Redis Cluster. Missing key.")
        # @barshaul decide on the implementation of finding key positions in commands which have a more complicated scheme
        # e.g. 'EVAL', 'EVALSHA','XREADGROUP', 'XREAD', 'XADD'

        command = args[0]
        key = args[1]

        # OBJECT command uses a special keyword as first positional argument
        if command == 'OBJECT':
            key = args[2]

        return self.keyslot(key)

    def execute_command(self, *args, **kwargs):
        """
        Wrapper for CLUSTERDOWN error handling.

        It will try the number of times specified by the config option "self.cluster_down_retry_attempts"
        which defaults to 3 unless manually configured.

        If it reaches the number of times, the command will raises ClusterDownException.
        """
        # Get Redis connection of the target nodes to send the command to
        target_nodes = kwargs.pop('target_nodes', None)
        for _ in range(0, self.cluster_down_retry_attempts):
            if not target_nodes:
                target_nodes = self._determine_nodes(*args, **kwargs)
            else:
                if type(target_nodes) is ClusterNode:
                    # allows to pass a single node as a variable
                    target_nodes = [target_nodes]
            try:
                res = {}
                for node in target_nodes:
                    res[node.name] = self._execute_command(node, *args, **kwargs)
                # When we execute the command on a single node, we can remove the dictionary and return a single response
                return res if len(res) > 1 else res[node.name]
            except ClusterDownError:
                # Try again with the new cluster setup. All other errors
                # should be raised.
                pass


        # If it fails the configured number of times then raise exception back to caller of this method
        raise ClusterDownError("CLUSTERDOWN error. Unable to rebuild the cluster")

    def _execute_command(self, target_node, *args, **kwargs):
        """
        Send a command to a node in the cluster
        """
        command = args[0]
        redis_node = None
        connection = None
        redirect_addr = None
        asking = False
        try_random_node = False
        ttl = int(self.RedisClusterRequestTTL)
        connection_error_retry_counter = 0
        updated_cache = False

        while ttl > 0:
            ttl -= 1

            try:
                if asking:
                    target_node = self.get_node(redirect_addr)
                elif try_random_node:
                    target_node = self.get_random_node()
                    try_random_node = False
                elif updated_cache:
                    # MOVED occurred and the cache was updated, refresh the target node
                    slot = self.determine_slot(*args)
                    target_node = self.nodes_manager.get_node_from_slot(slot)
                    updated_cache = False

                redis_node = self.get_redis_connection(target_node)
                connection = get_connection(redis_node, *args, **kwargs)

                if asking:
                    connection.send_command('ASKING')
                    redis_node.parse_response(connection, "ASKING", **kwargs)
                    asking = False
                if target_node.server_type == REPLICA:
                    # Ask read replica to accept reads (see https://redis.io/commands/readonly)
                    # TODO: handle errors from this response
                    connection.send_command('READONLY')
                    redis_node.parse_response(connection, 'READONLY', **kwargs)

                connection.send_command(*args)
                return redis_node.parse_response(connection, command, **kwargs)
            except (RedisClusterException, BusyLoadingError):
                warnings.warn("RedisClusterException || BusyLoadingError")
                raise
            except ConnectionError:
                warnings.warn("ConnectionError")

                # ConnectionError can also be raised if we couldn't get a connection
                # from the pool before timing out, so check that this is an actual
                # connection before attempting to disconnect.
                if connection is not None:
                    connection.disconnect()
                connection_error_retry_counter += 1

                # Give the node 0.1 seconds to get back up and retry again with same
                # node and configuration. After 5 attempts then try to reinitialize
                # the cluster and see if the nodes configuration has changed or not
                if connection_error_retry_counter < 5:
                    time.sleep(0.25)
                else:
                    # Reset the counter back to 0 as it should have 5 new attempts
                    # after the client tries to reinitailize the cluster setup to the
                    # new configuration.
                    connection_error_retry_counter = 0

                    # Hard force of reinitialize of the node/slots setup
                    self.nodes_manager.initialize()
                    updated_cache = True
            except TimeoutError:
                warnings.warn("TimeoutError")
                connection.disconnect()

                if ttl < self.RedisClusterRequestTTL / 2:
                    time.sleep(0.05)
                else:
                    try_random_node = True
            except MovedError as e:
                # Reinitialize on ever x number of MovedError.
                # This counter will increase faster when the same client object
                # is shared between multiple threads. To reduce the frequency you
                # can set the variable 'reinitialize_steps' in the constructor.
                warnings.warn("MovedError")

                self.reinitialize_counter += 1
                if self._should_reinitialized():
                    self.nodes_manager.initialize()
                else:
                    self.nodes_manager.update_moved_exception(e)
                updated_cache = True
            except TryAgainError:
                warnings.warn("TryAgainError")

                if ttl < self.RedisClusterRequestTTL / 2:
                    time.sleep(0.05)
            except AskError as e:
                warnings.warn("AskError")

                redirect_addr, asking = get_node_name(host=e.host, port=e.port), True
            except BaseException as e:
                warnings.warn("BaseException")
                connection.disconnect()
                raise e
            except ClusterDownError as e:
                warnings.warn("ClusterDownError")
                # ClusterDownError can occur during a failover and to get self-healed,
                # we will try to reinitialize the cluster layout and retry executing the command
                time.sleep(0.05)
                self.nodes_manager.initialize()
                raise e
            finally:
                if connection is not None:
                    redis_node.connection_pool.release(connection)

        raise ClusterError('TTL exhausted.')


class ClusterNode(object):
    def __init__(self, host, port, server_type=None, redis_connection=None):
        if host == 'localhost':
            host = socket.gethostbyname(host)

        self.host = host
        self.port = port
        self.name = f'{host}:{port}'
        self.server_type = server_type
        self.redis_connection = redis_connection

    def __repr__(self):
        return f'host={self.host},port={self.port},name={self.name},server_type={self.server_type}'

    def __eq__(self, obj):
        return isinstance(obj, ClusterNode) and obj.name == self.name


class NodesManager:
    def __init__(self, startup_nodes=None, skip_full_coverage_check=False, **kwargs):
        self.nodes_cache = {}
        self.slots_cache = {}
        self.startup_nodes = {}
        self.populate_startup_nodes(startup_nodes)
        self._skip_full_coverage_check = skip_full_coverage_check
        self.reset_connections = False
        self._moved_exception = None
        self.connection_kwargs = kwargs
        # Currently we do not support passing connection classes,
        # we use ClusterConnection by default or SSLClusterConnection for SSL
        self.initialize(**kwargs)

    def update_moved_exception(self, exception):
        self._moved_exception = 'exception'

    def update_node_data(self, host, port, server_type=None):
        """
        Update data for a node.
        """
        updated_node = ClusterNode(host, port, server_type)
        self.nodes_cache[updated_node.name] = updated_node

        return updated_node

    def _update_moved_slots(self):
        e = self._moved_exception
        new_primary = self.update_node_data(e.host, e.port, server_type=PRIMARY)
        if new_primary in self.slots_cache[e.slot_id]:
            # The MOVED error resulted from a failover, and the new slot owner had previously been a replica.
            old_primary = self.slots_cache[e.slot_id][0]
            # Update the old primary to be a replica and add it to the end of the slot's node list
            new_replica = self.update_node_data(old_primary.host, old_primary.port, server_type=REPLICA)
            self.slots_cache[e.slot_id].append(new_replica)
            # Remove the old replica, which is now a primary, from the slot's node list
            self.slots_cache[e.slot_id].remove(new_primary)
        # Override the old primary with the new one
        self.slots_cache[e.slot_id][0] = new_primary
        # Reset moved_exception
        self._moved_exception = None

    def get_node_from_slot(self, slot, server_type=None):
        """
        """
        try:
            if self._moved_exception:
                self._update_moved_slots()
            if server_type is None or server_type == PRIMARY or len(self.slots_cache[slot]) == 1:
                # return a primary
                target_node = self.slots_cache[slot][0]
            else:
                # return a replica
                # randomly choose one of the replicas
                replica_idx = random.randint(1, len(self.slots_cache[slot]) - 1)
                target_node = self.slots_cache[slot][replica_idx]
            return target_node
        except KeyError:
            raise SlotNotCoveredError(f'Slot "{slot}" not covered by the cluster. "skip_full_coverage_check={self._skip_full_coverage_check}"')

    def get_nodes_by_server_type(self, server_type):

        return [node for node in self.nodes_cache.values() if node.server_type == server_type]

    def populate_startup_nodes(self, nodes):
        """
        Populate all startup nodes and filters out any duplicates
        """
        for n in nodes:
            self.startup_nodes[n.name] = n

    def cluster_require_full_coverage(self, cluster_nodes):
        """
        if exists 'cluster-require-full-coverage no' config on redis servers,
        then even all slots are not covered, cluster still will be able to
        respond
        """

        def node_require_full_coverage(node):
            try:
                return "yes" in node.redis_connection.config_get("cluster-require-full-coverage").values()
            except ConnectionError:
                return False
            except Exception as e:
                raise RedisClusterException(
                    f'ERROR sending "config get cluster-require-full-coverage" command to redis server: {node.name}, {e}')

        # at least one node should have cluster-require-full-coverage yes
        return any(node_require_full_coverage(node) for node in cluster_nodes.values())

    def check_slots_coverage(self, cluster_nodes, slots_cache):
        if not self._skip_full_coverage_check and self.cluster_require_full_coverage(cluster_nodes):
            # Validate if all slots are covered or if we should try next startup node
            for i in range(0, REDIS_CLUSTER_HASH_SLOTS):
                if i not in slots_cache:
                    return False
        return True

    def create_redis_connections(self, nodes):
        """
        This function will create a redis connection to all nodes in :nodes:
        """
        for node in nodes:
            if node.redis_connection is None or self.reset_connections:
                # if reset_connections is set to true, create a new connection
                node.redis_connection = Redis(host=node.host, port=node.port,
                                              decode_responses=True,
                                              **self.connection_kwargs)

    def initialize(self, **kwargs):
        """
        Initializes the nodes cache, slots cache and redis connections.
        :startup_nodes:
            Responsible for discovering other nodes in the cluster
        """
        tmp_nodes_cache = {}
        tmp_slots = {}
        disagreements = []
        startup_nodes_reachable = False

        for startup_node in self.startup_nodes.values():
            try:
                if startup_node.redis_connection:
                    r = startup_node.redis_connection
                else:
                    # Create a new Redis connection
                    r = Redis(host=startup_node.host, port=startup_node.port, decode_responses=True, **kwargs)
                    self.startup_nodes[startup_node.name].redis_connection = r
                cluster_slots = r.execute_command("cluster", "slots")
                startup_nodes_reachable = True
            except (ConnectionError, TimeoutError):
                continue
            except ResponseError as e:
                warnings.warn("ReseponseError sending 'cluster slots' to redis server")

                # Isn't a cluster connection, so it won't parse these exceptions automatically
                message = e.__str__()
                if 'CLUSTERDOWN' in message or 'MASTERDOWN' in message:
                    continue
                else:
                    raise RedisClusterException(
                        f'ERROR sending "cluster slots" command to redis server: {startup_node}')
            except Exception:
                raise RedisClusterException(
                    f'ERROR sending "cluster slots" command to redis server: {startup_node}')

            # If there's only one server in the cluster, its ``host`` is ''
            # Fix it to the host in startup_nodes
            if len(cluster_slots) == 1 and len(cluster_slots[0][2][0]) == 0 and len(self.startup_nodes) == 1:
                cluster_slots[0][2][0] = startup_node.host

            # No need to decode response because Redis should handle that for us...
            for slot in cluster_slots:
                primary_node = slot[2]
                host = primary_node[0]
                if host == '':
                    host = startup_node.host
                port = int(primary_node[1])

                # primary_node = self.remap_internal_node_object(primary_node)

                target_node = ClusterNode(host, port, PRIMARY)
                # add this node to the nodes cache
                tmp_nodes_cache[target_node.name] = target_node

                for i in range(int(slot[0]), int(slot[1]) + 1):
                    if i not in tmp_slots:
                        tmp_slots[i] = [target_node]
                        replica_nodes = [slot[j] for j in range(3, len(slot))]

                        for replica_node in replica_nodes:
                            host = replica_node[0]
                            port = replica_node[1]
                            # replica_node = self.remap_internal_node_object(replica_node)
                            target_replica_node = ClusterNode(host, port, REPLICA)
                            tmp_slots[i].append(target_replica_node)
                            # add this node to the nodes cache
                            tmp_nodes_cache[target_replica_node.name] = target_replica_node
                    else:
                        # Validate that 2 nodes want to use the same slot cache setup
                        if tmp_slots[i][0].name != target_node.name:
                            disagreements.append(f'{tmp_slots[i][0].name} vs {target_node.name} on slot: {i}')

                            if len(disagreements) > 5:
                                raise RedisClusterException(
                                    f'startup_nodes could not agree on a valid slots cache: {", ".join(disagreements)}')

        if not startup_nodes_reachable:
            raise RedisClusterException(
                "Redis Cluster cannot be connected. Please provide at least one reachable node.")

        # Create Redis connections to all nodes
        self.create_redis_connections(list(tmp_nodes_cache.values()))

        if not self.check_slots_coverage(tmp_nodes_cache, tmp_slots):
            raise RedisClusterException(
                f'All slots are not covered after query all startup_nodes. {len(self.slots_cache)} of {REDIS_CLUSTER_HASH_SLOTS} covered...')

        # Switch the 'reset_connections' flag off if needed
        self.reset_connections = False
        # Set the tmp variables to the real variables
        self.nodes_cache = tmp_nodes_cache
        self.slots_cache = tmp_slots
        # Populate the startup nodes with all discovered nodes
        self.populate_startup_nodes(self.nodes_cache.values())

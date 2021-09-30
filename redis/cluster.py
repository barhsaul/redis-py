import copy
import random
import socket
import time
import warnings

from collections import OrderedDict
from redis.client import CaseInsensitiveDict, Redis, parse_info, PubSub
from redis.commands import (
    ClusterCommands,
    DataAccessCommands,
    ClusterManagementCommands,
    CommandsParser
)
from redis.connection import DefaultParser, ConnectionPool, Encoder, parse_url
from redis.crc import key_slot
from redis.exceptions import (
    AskError,
    BusyLoadingError,
    ClusterCrossSlotError,
    ClusterDownError,
    ClusterError,
    DataError,
    MasterDownError,
    MovedError,
    RedisClusterException,
    ResponseError,
    SlotNotCoveredError,
    TimeoutError,
    TryAgainError,
)
from redis.utils import (
    dict_merge,
    list_keys_to_dict,
    merge_result,
    str_if_bytes
)


def get_node_name(host, port):
    return '{0}:{1}'.format(host, port)


def get_connection(redis_node, *args, **options):
    return redis_node.connection or redis_node.connection_pool.get_connection(
        args[0], **options
    )


def parse_pubsub_numsub(command, res, **options):
    numsub_d = OrderedDict()
    for numsub_tups in res.values():
        for channel, numsubbed in numsub_tups:
            try:
                numsub_d[channel] += numsubbed
            except KeyError:
                numsub_d[channel] = numsubbed

    ret_numsub = [
        (channel, numsub)
        for channel, numsub in numsub_d.items()
    ]
    return ret_numsub


def parse_cluster_slots(resp, **options):
    current_host = options.get('current_host', '')

    def fix_server(*args):
        return str_if_bytes(args[0]) or current_host, args[1]

    slots = {}
    for slot in resp:
        start, end, master = slot[:3]
        slaves = slot[3:]
        slots[start, end] = {
            'master': fix_server(*master),
            'slaves': [fix_server(*slave) for slave in slaves],
        }

    return slots


def parse_cluster_nodes(resp, **options):
    """
    @see: http://redis.io/commands/cluster-nodes  # string
    @see: http://redis.io/commands/cluster-slaves # list of string
    """
    resp = str_if_bytes(resp)
    current_host = options.get('current_host', '')

    def parse_slots(s):
        slots, migrations = [], []
        for r in s.split(' '):
            if '->-' in r:
                slot_id, dst_node_id = r[1:-1].split('->-', 1)
                migrations.append({
                    'slot': int(slot_id),
                    'node_id': dst_node_id,
                    'state': 'migrating'
                })
            elif '-<-' in r:
                slot_id, src_node_id = r[1:-1].split('-<-', 1)
                migrations.append({
                    'slot': int(slot_id),
                    'node_id': src_node_id,
                    'state': 'importing'
                })
            elif '-' in r:
                start, end = r.split('-')
                slots.extend(range(int(start), int(end) + 1))
            else:
                slots.append(int(r))

        return slots, migrations

    resp = resp.splitlines()
    nodes = []
    for line in resp:
        parts = line.split(' ', 8)
        self_id, addr, flags, master_id, ping_sent, \
            pong_recv, config_epoch, link_state = parts[:8]

        host, ports = addr.rsplit(':', 1)
        port, _, cluster_port = ports.partition('@')

        node = {
            'id': self_id,
            'host': host or current_host,
            'port': int(port),
            'cluster-bus-port': int(
                cluster_port) if cluster_port else 10000 + int(port),
            'flags': tuple(flags.split(',')),
            'master': master_id if master_id != '-' else None,
            'ping-sent': int(ping_sent),
            'pong-recv': int(pong_recv),
            'link-state': link_state,
            'slots': [],
            'migrations': [],
        }

        if len(parts) >= 9:
            slots, migrations = parse_slots(parts[8])
            node['slots'], node['migrations'] = tuple(slots), migrations

        nodes.append(node)

    return nodes


PRIMARY = "primary"
REPLICA = "replica"
ALL_PRIMARIES = "all-primaries"
ALL_REPLICAS = "all-replicas"
ALL_NODES = "all-nodes"
RANDOM = "random"
SLOT_ID = "slot-id"

REDIS_ALLOWED_KEYS = (
    "charset",
    "connection_class",
    "connection_pool",
    "db",
    "decode_responses",
    "encoding",
    "encoding_errors",
    "errors",
    "host",
    "max_connections",
    "redis_connect_func",
    "password",
    "port",
    "retry_on_timeout",
    "socket_connect_timeout",
    "socket_keepalive",
    "socket_keepalive_options",
    "socket_timeout",
    "ssl",
    "ssl_ca_certs",
    "ssl_certfile",
    "ssl_cert_reqs",
    "ssl_keyfile",
    "unix_socket_path",
    "username",
)
KWARGS_DISABLED_KEYS = (
    "host",
    "port",
)

# Not complete, but covers the major ones
# https://redis.io/commands
READ_COMMANDS = frozenset([
    "BITCOUNT",
    "BITPOS",
    "EXISTS",
    "GEODIST",
    "GEOHASH",
    "GEOPOS",
    "GEORADIUS",
    "GEORADIUSBYMEMBER",
    "GET",
    "GETBIT",
    "GETRANGE",
    "HEXISTS",
    "HGET",
    "HGETALL",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HSTRLEN",
    "HVALS",
    "KEYS",
    "LINDEX",
    "LLEN",
    "LRANGE",
    "MGET",
    "PTTL",
    "RANDOMKEY",
    "SCARD",
    "SDIFF",
    "SINTER",
    "SISMEMBER",
    "SMEMBERS",
    "SRANDMEMBER",
    "STRLEN",
    "SUNION",
    "TTL",
    "ZCARD",
    "ZCOUNT",
    "ZRANGE",
    "ZSCORE",
])

# Redis Cluster's key space is divided into 16384 slots.
# For more information see: https://github.com/redis/redis/issues/2576
REDIS_CLUSTER_HASH_SLOTS = 16384


def cleanup_kwargs(**kwargs):
    """
    Remove unsupported or disabled keys from kwargs
    """
    connection_kwargs = {
        k: v
        for k, v in kwargs.items()
        if k in REDIS_ALLOWED_KEYS and k not in KWARGS_DISABLED_KEYS
    }

    return connection_kwargs


class ClusterParser(DefaultParser):
    EXCEPTION_CLASSES = dict_merge(
        DefaultParser.EXCEPTION_CLASSES, {
            'ASK': AskError,
            'TRYAGAIN': TryAgainError,
            'MOVED': MovedError,
            'CLUSTERDOWN': ClusterDownError,
            'CROSSSLOT': ClusterCrossSlotError,
            'MASTERDOWN': MasterDownError,
        })


class RedisCluster(ClusterCommands, DataAccessCommands,
                   ClusterManagementCommands, object):
    RedisClusterRequestTTL = 16
    NODES_FLAGS = dict_merge(
        list_keys_to_dict(
            [
                "CLIENT LIST",
                "CONFIG SET",
                "TIME",
                "PUBSUB CHANNELS",
                "PUBSUB NUMPAT",
                "PUBSUB NUMSUB",
                "PING"
            ],
            ALL_NODES,
        ),
        list_keys_to_dict(
            [
                "KEYS",
                "SCAN",
                "FLUSHALL",
                "FLUSHDB"
            ],
            ALL_PRIMARIES,
        ),
        list_keys_to_dict(
            [
                "READONLY",
                "READWRITE",
            ],
            ALL_REPLICAS,
        ),
        list_keys_to_dict(
            [
                "CLUSTER NODES",
                "CLUSTER REPLICAS",
                "CLUSTER SLOTS",
                "RANDOMKEY",
                "COMMAND",
                "DEBUG",
            ],
            RANDOM,
        ),
        list_keys_to_dict(
            [
                "CLUSTER COUNTKEYSINSLOT",
                "CLUSTER GETKEYSINSLOT",
                "CLUSTER SETSLOT",
            ],
            SLOT_ID,
        ),
    )

    CLUSTER_COMMANDS_RESPONSE_CALLBACKS = {
        'CLUSTER ADDSLOTS': bool,
        'CLUSTER COUNT-FAILURE-REPORTS': int,
        'CLUSTER COUNTKEYSINSLOT': int,
        'CLUSTER DELSLOTS': bool,
        'CLUSTER FAILOVER': bool,
        'CLUSTER FORGET': bool,
        'CLUSTER GETKEYSINSLOT': list,
        'CLUSTER INFO': parse_info,
        'CLUSTER KEYSLOT': int,
        'CLUSTER MEET': bool,
        'CLUSTER NODES': parse_cluster_nodes,
        'CLUSTER REPLICATE': bool,
        'CLUSTER RESET': bool,
        'CLUSTER SAVECONFIG': bool,
        'CLUSTER SET-CONFIG-EPOCH': bool,
        'CLUSTER SETSLOT': bool,
        'CLUSTER SLAVES': parse_cluster_nodes,
        'CLUSTER SLOTS': parse_cluster_slots,
        'ASKING': bool,
        'READONLY': bool,
        'READWRITE': bool,
    }

    RESULT_CALLBACKS = dict_merge(
        list_keys_to_dict([
            "PUBSUB NUMSUB",
        ], parse_pubsub_numsub),
        list_keys_to_dict([
            "PUBSUB NUMPAT",
        ], lambda command, res: sum(list(res.values()))),
        list_keys_to_dict([
            "KEYS",
            "PUBSUB CHANNELS",
        ], merge_result),
        list_keys_to_dict([
            "PING",
        ], lambda command, res: all(res.values())),
    )

    def __init__(
            self,
            host=None,
            port=6379,
            startup_nodes=None,
            cluster_error_retry_attempts=3,
            skip_full_coverage_check=False,
            reinitialize_steps=10,
            read_from_replicas=False,
            url=None,
            **kwargs
    ):
        """
        :startup_nodes: 'list[ClusterNode]'
            List of nodes from which initial bootstrapping can be done
        :host: 'str'
            Can be used to point to a startup node
        :port: 'int'
            Can be used to point to a startup node
        :skip_full_coverage_check: 'bool'
            Skips the check of cluster-require-full-coverage config, useful for
            clusters without the CONFIG command (like aws)
       :read_from_replicas: 'bool'
            Enable read from replicas in READONLY mode. You can read possibly
            stale data.
            When set to true, read commands will be assigned between the
            primary and its replications in a Round-Robin manner.
        :cluster_error_retry_attempts: 'int'

        :**kwargs:
            Extra arguments that will be sent into Redis instance when created
            (See Official redis-py doc for supported kwargs
        [https://github.com/andymccurdy/redis-py/blob/master/redis/client.py])
            Some kwargs are not supported and will raise a
            RedisClusterException:
                - db (Redis do not support database SELECT in cluster mode)
        """

        if startup_nodes is None:
            startup_nodes = []

        if "db" in kwargs:
            # Argument 'db' is not possible to use in cluster mode
            raise RedisClusterException(
                "Argument 'db' is not possible to use in cluster mode"
            )

        # Get the startup node/s
        from_url = False
        if url is not None:
            from_url = True
            url_options = parse_url(url)
            if "path" in url_options:
                raise RedisClusterException(
                    "RedisCluster does not currently support Unix Domain "
                    "Socket connections")
            if "db" in url_options and url_options["db"] != 0:
                # Argument 'db' is not possible to use in cluster mode
                raise RedisClusterException(
                    "A ``db`` querystring option can only be 0 in cluster mode"
                )
            kwargs.update(url_options)
            startup_nodes.append(ClusterNode(kwargs['host'], kwargs['port']))
        elif host is not None and port is not None:
            startup_nodes.append(ClusterNode(host, port))
        elif len(startup_nodes) == 0:
            # No startup node was provided
            raise RedisClusterException(
                "RedisCluster requires at least one node to discover the "
                "cluster. Please provide one of the followings:\n"
                "1. host and port, for example:\n"
                " RedisCluster(host='localhost', port=6379)\n"
                "2. list of startup nodes, for example:\n"
                " RedisCluster(startup_nodes=[ClusterNode('localhost', 6379),"
                " ClusterNode('localhost', 6378)])")

        # Update the connection arguments
        # Whenever a new connection is established, RedisCluster's on_connect
        # method should be run
        kwargs.update({"redis_connect_func": self.on_connect})
        kwargs = cleanup_kwargs(**kwargs)

        self.encoder = Encoder(
            kwargs.get("encoding", "utf-8"),
            kwargs.get("encoding_errors", "strict"),
            kwargs.get("decode_responses", False),
        )
        self.cluster_error_retry_attempts = cluster_error_retry_attempts
        self.nodes_flags = self.__class__.NODES_FLAGS.copy()
        self.read_from_replicas = read_from_replicas
        self.reinitialize_counter = 0
        self.reinitialize_steps = reinitialize_steps
        self.nodes_manager = None
        self.nodes_manager = NodesManager(
            startup_nodes=startup_nodes,
            from_url=from_url,
            skip_full_coverage_check=skip_full_coverage_check,
            **kwargs,
        )

        self.response_callbacks = CaseInsensitiveDict(dict_merge(
            Redis.RESPONSE_CALLBACKS,
            self.__class__.CLUSTER_COMMANDS_RESPONSE_CALLBACKS))
        self.result_callbacks = self.__class__.RESULT_CALLBACKS
        self.commands_parser = CommandsParser(self.get_random_node().
                                              redis_connection)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def disconnect_connection_pools(self):
        for node in self.get_nodes():
            if node.redis_connection:
                node.redis_connection.connection_pool.disconnect()

    @classmethod
    def from_url(cls, url, **kwargs):
        """
        Return a Redis client object configured from the given URL

        For example::

            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[[username]:[password]]@/path/to/socket.sock?db=0

        Three URL schemes are supported:

        - `redis://` creates a TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/redis>
        - `rediss://` creates a SSL wrapped TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>
        - ``unix://``: creates a Unix Domain Socket connection.

        The username, password, hostname, path and all querystring values
        are passed through urllib.parse.unquote in order to replace any
        percent-encoded values with their corresponding characters.

        There are several ways to specify a database number. The first value
        found will be used:
            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// or rediss:// schemes, the path argument
               of the url, e.g. redis://localhost/0
            3. A ``db`` keyword argument to this function.

        If none of these options are specified, the default db=0 is used.

        All querystring options are cast to their appropriate Python types.
        Boolean arguments can be specified with string values "True"/"False"
        or "Yes"/"No". Values that cannot be properly cast cause a
        ``ValueError`` to be raised. Once parsed, the querystring arguments
        and keyword arguments are passed to the ``ConnectionPool``'s
        class initializer. In the case of conflicting arguments, querystring
        arguments always win.

        """
        return cls(url=url, **kwargs)

    def on_connect(self, connection):
        """
        Initialize the connection, authenticate and select a database and send
         READONLY if it is set during object initialization.
        """
        connection.set_parser(ClusterParser)
        connection.on_connect()

        if self.read_from_replicas:
            # Sending READONLY command to server to configure connection as
            # readonly. Since each cluster node may change its server type due
            # to a failover, we should establish a READONLY connection
            # regardless of the server type. If this is a primary connection,
            # READONLY would not affect executing write commands.
            connection.send_command('READONLY')
            if str_if_bytes(connection.read_response()) != 'OK':
                raise ConnectionError('READONLY command failed')

    def get_redis_connection(self, node):
        if not node.redis_connection:
            self.nodes_manager.create_redis_connections([node])
        return node.redis_connection

    def get_node(self, node_name=None, host=None, port=None):
        return self.nodes_manager.get_node(node_name, host, port)

    def get_primaries(self):
        return self.nodes_manager.get_nodes_by_server_type(PRIMARY)

    def get_replicas(self):
        return self.nodes_manager.get_nodes_by_server_type(REPLICA)

    def get_random_node(self):
        return random.choice(list(self.nodes_manager.nodes_cache.values()))

    def get_nodes(self):
        return list(self.nodes_manager.nodes_cache.values())

    def pubsub(self, node=None, host=None, port=None, **kwargs):
        """
        Allows passing a ClusterNode, or host&port, to get a pubsub instance
        connected to the specified node
        """
        return ClusterPubSub(self, node=node, host=host, port=port, **kwargs)

    def _determine_nodes(self, *args, **kwargs):
        command = args[0]
        node_flag = self.nodes_flags.get(command)

        if node_flag == RANDOM:
            return [self.get_random_node()]
        elif node_flag == ALL_PRIMARIES:
            return self.get_primaries()
        elif node_flag == ALL_REPLICAS:
            return self.get_replicas()
        elif node_flag == ALL_NODES:
            return self.get_nodes()
        else:
            # get the node that holds the key's slot
            slot = self.determine_slot(*args)
            return [self.nodes_manager.
                    get_node_from_slot(slot, self.read_from_replicas
                                       and command in READ_COMMANDS)]

    def _increment_reinitialize_counter(self, count=1):
        # In order not to reinitialize the cluster, the user can set
        # reinitialize_steps to 0.
        for i in range(min(1, self.reinitialize_steps)):
            self.reinitialize_counter += count
            if self.reinitialize_counter % self.reinitialize_steps == 0:
                self.initialize()

    def _should_reinitialized(self):
        # In order not to reinitialize the cluster, the user can set
        # reinitialize_steps to 0.
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
        return key_slot(k, REDIS_CLUSTER_HASH_SLOTS)

    def determine_slot(self, *args):
        """
        figure out what slot based on command and args
        """
        keys = self.commands_parser.get_keys(*args)
        if keys is None or len(keys) == 0:
            raise RedisClusterException(
                "No way to dispatch this command to Redis Cluster. "
                "Missing key. command: {0}".format(args)
            )

        if len(keys) > 1:
            # multi-key command, we need to make sure all keys are mapped to
            # the same slot
            slots = {self.keyslot(key) for key in keys}
            if len(slots) != 1:
                raise RedisClusterException("{0} - all keys must map to the "
                                            "same key slot".format(args[0]))
            return slots.pop()
        else:
            # single key command
            return self.keyslot(keys[0])

    def execute_command(self, *args, **kwargs):
        """
        Wrapper for CLUSTERDOWN error handling.

        It will try the number of times specified by the config option
        "self.cluster_down_retry_attempts" which defaults to 3 unless manually
        configured.

        If it reaches the number of times, the command will raises
        ClusterDownException.
        """
        target_nodes = kwargs.pop("target_nodes", None)
        target_nodes_specified = target_nodes is not None
        if target_nodes_specified:
            if isinstance(target_nodes, ClusterNode):
                # Supports passing a single ClusterNode as a variable
                target_nodes = [target_nodes]
            elif isinstance(target_nodes, dict):
                # Supports dictionaries of the format {node_name: node}.
                # It enables to execute commands with multi nodes as follows:
                # rc.cluster_save_config(rc.get_primaries())
                target_nodes = target_nodes.values()
        # If ClusterDownError/ConnectionError were thrown, the nodes
        # and slots cache were reinitialized. We will retry executing the
        # command with the updated cluster setup only when the target nodes
        # can be determined again with the new cache tables. Therefore,
        # when target nodes were passed to this function, we cannot retry
        # the command execution since the nodes may not be valid anymore
        # after the tables were reinitialized. So in case of passed target
        # nodes, retry_attempts will be set to 1.
        retry_attempts = 1 if target_nodes_specified else \
            self.cluster_error_retry_attempts
        exception = None
        for _ in range(0, retry_attempts):
            try:
                res = {}
                if not target_nodes_specified:
                    # Determine the nodes to execute the command on
                    target_nodes = self._determine_nodes(*args, **kwargs)
                    if not target_nodes:
                        raise RedisClusterException(
                            "No targets were found to execute"
                            " {} command on".format(args))
                for node in target_nodes:
                    res[node.name] = self._execute_command(
                        node, *args, **kwargs)
                # Return the processed result
                return self.process_result(args[0], res, **kwargs)
            except (ClusterDownError, ConnectionError) as e:
                # Try again with the new cluster setup. All other errors
                # should be raised.
                exception = e

        # If it fails the configured number of times then raise exception back
        # to caller of this method
        raise exception

    def _execute_command(self, target_node, *args, **kwargs):
        """
        Send a command to a node in the cluster
        """
        command = args[0]
        redis_node = None
        connection = None
        redirect_addr = None
        asking = False
        moved = False
        ttl = int(self.RedisClusterRequestTTL)
        connection_error_retry_counter = 0

        while ttl > 0:
            ttl -= 1

            try:
                if asking:
                    target_node = self.get_node(redirect_addr)
                elif moved:
                    # MOVED occurred and the slots cache was updated,
                    # refresh the target node
                    slot = self.determine_slot(*args)
                    target_node = self.nodes_manager. \
                        get_node_from_slot(slot, self.read_from_replicas and
                                           command in READ_COMMANDS)
                    moved = False

                redis_node = self.get_redis_connection(target_node)
                connection = get_connection(redis_node, *args, **kwargs)
                if asking:
                    connection.send_command("ASKING")
                    redis_node.parse_response(connection, "ASKING", **kwargs)
                    asking = False

                connection.send_command(*args)
                return redis_node.parse_response(connection, command, **kwargs)

            except (RedisClusterException, BusyLoadingError):
                warnings.warn("RedisClusterException || BusyLoadingError")
                raise
            except ConnectionError:
                warnings.warn("ConnectionError")
                # ConnectionError can also be raised if we couldn't get a
                # connection from the pool before timing out, so check that
                # this is an actual connection before attempting to disconnect.
                if connection is not None:
                    connection.disconnect()
                connection_error_retry_counter += 1

                # Give the node 0.25 seconds to get back up and retry again
                # with same node and configuration. After 5 attempts then try
                # to reinitialize the cluster and see if the nodes
                # configuration has changed or not
                if connection_error_retry_counter < 5:
                    time.sleep(0.25)
                else:
                    # Hard force of reinitialize of the node/slots setup
                    self.nodes_manager.initialize()
                    raise
            except TimeoutError:
                warnings.warn("TimeoutError")
                if connection is not None:
                    connection.disconnect()

                if ttl < self.RedisClusterRequestTTL / 2:
                    time.sleep(0.05)
            except MovedError as e:
                # First, we will try to patch the slots/nodes cache with the
                # redirected node output and try again. If MovedError exceeds
                # 'reinitialize_steps' number of times, we will force
                # reinitializing the tables, and then try again.
                # 'reinitialize_steps' counter will increase faster when the
                # same client object is shared between multiple threads. To
                # reduce the frequency you can set this variable in the
                # RedisCluster constructor.
                warnings.warn("MovedError")
                self.reinitialize_counter += 1
                if self._should_reinitialized():
                    self.nodes_manager.initialize()
                else:
                    self.nodes_manager.update_moved_exception(e)
                    moved = True
            except TryAgainError:
                warnings.warn("TryAgainError")

                if ttl < self.RedisClusterRequestTTL / 2:
                    time.sleep(0.05)
            except AskError as e:
                warnings.warn("AskError")

                redirect_addr = get_node_name(host=e.host, port=e.port)
                asking = True
            except ClusterDownError as e:
                warnings.warn("ClusterDownError")
                # ClusterDownError can occur during a failover and to get
                # self-healed, we will try to reinitialize the cluster layout
                # and retry executing the command
                time.sleep(0.05)
                self.nodes_manager.initialize()
                raise e
            except BaseException as e:
                warnings.warn("BaseException")
                if connection:
                    connection.disconnect()
                raise e
            finally:
                if connection is not None:
                    redis_node.connection_pool.release(connection)

        raise ClusterError("TTL exhausted.")

    def close(self):
        try:
            self.nodes_manager.close()
        except AttributeError:
            # RedisCluster's __init__ can fail before nodes_manager is set
            pass

    def process_result(self, command, res, **kwargs):
        """
        `res` is a dict with the following structure:
            Dict(NodeName, CommandResult)
        """
        if not isinstance(res, dict):
            raise ValueError('The result should be of dict type')

        if command in self.result_callbacks:
            return self.result_callbacks[command](command, res, **kwargs)
        elif len(res) == 1:
            # When we execute the command on a single node, we can
            # remove the dictionary and return a single response
            return list(res.values())[0]
        else:
            return res


class ClusterNode(object):
    def __init__(self, host, port, server_type=None, redis_connection=None):
        if host == "localhost":
            host = socket.gethostbyname(host)

        self.host = host
        self.port = port
        self.name = get_node_name(host, port)
        self.server_type = server_type
        self.redis_connection = redis_connection

    def __repr__(self):
        return 'host={0},port={1},' \
               'name={2},server_type={3}' \
            .format(self.host,
                    self.port,
                    self.name,
                    self.server_type)

    def __eq__(self, obj):
        return isinstance(obj, ClusterNode) and obj.name == self.name


class LoadBalancer:
    """
    Round-Robin Load Balancing
    """

    def __init__(self, bucket_size, start_index=0):
        self.slot_idx_list = [start_index] * bucket_size

    def get_server_index(self, slot, list_size):
        server_index = self.slot_idx_list[slot]
        # Update the index
        self.slot_idx_list[slot] = (server_index + 1) % list_size
        return server_index


class NodesManager:
    def __init__(self, startup_nodes, from_url=False,
                 skip_full_coverage_check=False, **kwargs):
        self.nodes_cache = {}
        self.slots_cache = {}
        self.startup_nodes = {}
        self.populate_startup_nodes(startup_nodes)
        self.from_url = from_url
        self._skip_full_coverage_check = skip_full_coverage_check
        self._moved_exception = None
        self.connection_kwargs = kwargs
        self.read_load_balancer = None
        self.initialize()

    def get_node(self, node_name=None, host=None, port=None):
        if node_name is None and (host is None or port is None):
            warnings.warn(
                "get_node requires one of the followings: "
                "1. node name "
                "2. host and port"
            )
            return None
        if host is not None and port is not None:
            if host == "localhost":
                host = socket.gethostbyname(host)
            node_name = get_node_name(host=host, port=port)
        return self.nodes_cache.get(node_name)

    def update_moved_exception(self, exception):
        self._moved_exception = exception

    def _update_moved_slots(self):
        e = self._moved_exception
        redirected_node = self.get_node(host=e.host, port=e.port)
        if redirected_node:
            if redirected_node.server_type is not PRIMARY:
                # Update the node's server type
                redirected_node.server_type = PRIMARY
        else:
            # This is a new node, we will add it to the nodes cache
            redirected_node = ClusterNode(e.host, e.port, PRIMARY)
            self.nodes_cache[redirected_node.name] = redirected_node
        if redirected_node in self.slots_cache[e.slot_id]:
            # The MOVED error resulted from a failover, and the new slot owner
            # had previously been a replica.
            old_primary = self.slots_cache[e.slot_id][0]
            # Update the old primary to be a replica and add it to the end of
            # the slot's node list
            old_primary.server_type = REPLICA
            self.slots_cache[e.slot_id].append(old_primary)
            # Remove the old replica, which is now a primary, from the slot's
            # node list
            self.slots_cache[e.slot_id].remove(redirected_node)
            # Override the old primary with the new one
            self.slots_cache[e.slot_id][0] = redirected_node
        else:
            # The new slot owner is a new server, or a server from a different
            # shard. We need to remove all current nodes from the slot's list
            # (including replications) and add just the new node.
            self.slots_cache[e.slot_id] = [redirected_node]
        # Reset moved_exception
        self._moved_exception = None

    def get_node_from_slot(self, slot, read_from_replicas=False,
                           server_type=None):
        """
        Gets a node that servers this hash slot
        """
        if self._moved_exception:
            self._update_moved_slots()
        if read_from_replicas:
            # get the server index in a Round-Robin manner
            node_idx = self.get_read_load_balancer().get_server_index(
                slot, len(self.slots_cache[slot]))
        elif (
                server_type is None
                or server_type == PRIMARY
                or len(self.slots_cache[slot]) == 1
        ):
            # return a primary
            node_idx = 0
        else:
            # return a replica
            # randomly choose one of the replicas
            node_idx = random.randint(
                1, len(self.slots_cache[slot]) - 1)
        try:
            target_node = self.slots_cache[slot][node_idx]
        except KeyError:
            raise SlotNotCoveredError(
                'Slot "{0}" not covered by the cluster. '
                '"skip_full_coverage_check={1}"'.format(
                    slot, self._skip_full_coverage_check)
            )
        return target_node

    def get_nodes_by_server_type(self, server_type):
        return [
            node
            for node in self.nodes_cache.values()
            if node.server_type == server_type
        ]

    def get_read_load_balancer(self):
        if self.read_load_balancer is None:
            self.read_load_balancer = LoadBalancer(REDIS_CLUSTER_HASH_SLOTS)
        return self.read_load_balancer

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
                return ("yes" in node.redis_connection.config_get(
                    "cluster-require-full-coverage").values()
                )
            except ConnectionError:
                return False
            except Exception as e:
                raise RedisClusterException(
                    'ERROR sending "config get cluster-require-full-coverage"'
                    ' command to redis server: {0}, {1}'.format(node.name, e)
                )

        # at least one node should have cluster-require-full-coverage yes
        return any(node_require_full_coverage(node)
                   for node in cluster_nodes.values())

    def check_slots_coverage(self, cluster_nodes, slots_cache):
        if not self._skip_full_coverage_check and \
                self.cluster_require_full_coverage(
                    cluster_nodes
                ):
            # Validate if all slots are covered or if we should try next
            # startup node
            for i in range(0, REDIS_CLUSTER_HASH_SLOTS):
                if i not in slots_cache:
                    return False
        return True

    def create_redis_connections(self, nodes):
        """
        This function will create a redis connection to all nodes in :nodes:
        """
        for node in nodes:
            if node.redis_connection is None:
                node.redis_connection = self.create_redis_node(
                    host=node.host,
                    port=node.port,
                    **self.connection_kwargs,
                )

    def create_redis_node(self, host, port, **kwargs):
        if self.from_url:
            # Create a redis node with a costumed connection pool
            kwargs.update({"host": host})
            kwargs.update({"port": port})
            connection_pool = ConnectionPool(**kwargs)
            r = Redis(
                connection_pool=connection_pool
            )
        else:
            r = Redis(
                host=host,
                port=port,
                **kwargs
            )
        return r

    def initialize(self):
        """
        Initializes the nodes cache, slots cache and redis connections.
        :startup_nodes:
            Responsible for discovering other nodes in the cluster
        """
        tmp_nodes_cache = {}
        tmp_slots = {}
        disagreements = []
        startup_nodes_reachable = False
        kwargs = self.connection_kwargs
        for startup_node in self.startup_nodes.values():
            try:
                if startup_node.redis_connection:
                    r = startup_node.redis_connection
                else:
                    # Create a new Redis connection and let Redis decode the
                    # responses so we won't need to handle that
                    copy_kwargs = copy.deepcopy(kwargs)
                    copy_kwargs.update({"decode_responses": True})
                    copy_kwargs.update({"encoding": "utf-8"})
                    r = self.create_redis_node(
                        startup_node.host, startup_node.port, **copy_kwargs)
                    self.startup_nodes[startup_node.name].redis_connection = r
                cluster_slots = r.execute_command("CLUSTER SLOTS")
                startup_nodes_reachable = True
            except (ConnectionError, TimeoutError):
                continue
            except ResponseError as e:
                warnings.warn(
                    'ReseponseError sending "cluster slots" to redis server')

                # Isn't a cluster connection, so it won't parse these
                # exceptions automatically
                message = e.__str__()
                if "CLUSTERDOWN" in message or "MASTERDOWN" in message:
                    continue
                else:
                    raise RedisClusterException(
                        'ERROR sending "cluster slots" command to redis '
                        'server: {0}. error: {1}'.format(
                            startup_node, message)
                    )
            except Exception as e:
                message = e.__str__()
                raise RedisClusterException(
                    'ERROR sending "cluster slots" command to redis '
                    'server: {0}. error: {1}'.format(
                        startup_node, message)
                )

            # If there's only one server in the cluster, its ``host`` is ''
            # Fix it to the host in startup_nodes
            if (len(cluster_slots) == 1
                    and len(cluster_slots[0][2][0]) == 0
                    and len(self.startup_nodes) == 1):
                cluster_slots[0][2][0] = startup_node.host

            for slot in cluster_slots:
                primary_node = slot[2]
                host = primary_node[0]
                if host == "":
                    host = startup_node.host
                port = int(primary_node[1])

                target_node = tmp_nodes_cache.get(get_node_name(host, port))
                if target_node is None:
                    target_node = ClusterNode(host, port, PRIMARY)
                # add this node to the nodes cache
                tmp_nodes_cache[target_node.name] = target_node

                for i in range(int(slot[0]), int(slot[1]) + 1):
                    if i not in tmp_slots:
                        tmp_slots[i] = []
                        tmp_slots[i].append(target_node)
                        replica_nodes = [slot[j] for j in range(3, len(slot))]

                        for replica_node in replica_nodes:
                            host = replica_node[0]
                            port = replica_node[1]

                            target_replica_node = tmp_nodes_cache.get(
                                get_node_name(host, port))
                            if target_replica_node is None:
                                target_replica_node = ClusterNode(
                                    host, port, REPLICA)
                            tmp_slots[i].append(target_replica_node)
                            # add this node to the nodes cache
                            tmp_nodes_cache[
                                target_replica_node.name
                            ] = target_replica_node
                    else:
                        # Validate that 2 nodes want to use the same slot cache
                        # setup
                        if tmp_slots[i][0].name != target_node.name:
                            disagreements.append(
                                '{0} vs {1} on slot: {2}'.format(
                                    tmp_slots[i][0].name, target_node.name, i)
                            )

                            if len(disagreements) > 5:
                                raise RedisClusterException(
                                    'startup_nodes could not agree on a valid'
                                    ' slots cache: {0}'.format(
                                        ", ".join(disagreements))
                                )

        if not startup_nodes_reachable:
            raise RedisClusterException(
                "Redis Cluster cannot be connected. Please provide at least "
                "one reachable node. "
            )
        # Create Redis connections to all nodes

        self.create_redis_connections(list(tmp_nodes_cache.values()))

        if not self.check_slots_coverage(tmp_nodes_cache, tmp_slots):
            raise RedisClusterException(
                'All slots are not covered after query all startup_nodes.'
                ' {0} of {1} covered...'.format(
                    len(self.slots_cache), REDIS_CLUSTER_HASH_SLOTS)
            )

        # Set the tmp variables to the real variables
        self.nodes_cache = tmp_nodes_cache
        self.slots_cache = tmp_slots
        # Populate the startup nodes with all discovered nodes
        self.populate_startup_nodes(self.nodes_cache.values())

    def close(self):
        for node in self.nodes_cache.values():
            if node.redis_connection:
                node.redis_connection.close()


class ClusterPubSub(PubSub):
    """
    Wrapper for PubSub class.

    IMPORTANT: before using ClusterPubSub, read about the known limitations
    with pubsub in Cluster mode and learn how to workaround them:
    https://redis-py-cluster.readthedocs.io/en/stable/pubsub.html
    """

    def __init__(self, redis_cluster, node=None, host=None, port=None,
                 **kwargs):
        """
        When a pubsub instance is created without specifying a node, a single
        node will be transparently chosen for the pubsub connection on the
        first command execution. The node will be determined by:
         1. Hashing the channel name in the request to find its keyslot
         2. Selecting a node that handles the keyslot: If read_from_replicas is
            set to true, a replica can be selected.
        """
        self.node = None
        connection_pool = None
        if host is not None and port is not None:
            node = redis_cluster.get_node(host=host, port=port)
            self.node = node
        if node is not None:
            if not isinstance(node, ClusterNode):
                raise DataError("'node' must be a ClusterNode")
            connection_pool = redis_cluster.get_redis_connection(node). \
                connection_pool
        self.cluster = redis_cluster
        super().__init__(**kwargs, connection_pool=connection_pool,
                         encoder=redis_cluster.encoder)

    def execute_command(self, *args, **kwargs):
        """
        Execute a publish/subscribe command.

        Taken code from redis-py and tweak to make it work within a cluster.
        """
        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

        if self.connection is None:
            if self.connection_pool is None:
                if len(args) > 1:
                    # Hash the first channel and get one of the nodes holding
                    # this slot
                    channel = args[1]
                    slot = self.cluster.keyslot(channel)
                    node = self.cluster.nodes_manager. \
                        get_node_from_slot(slot, self.cluster.
                                           read_from_replicas)
                else:
                    # Get a random node
                    node = self.cluster.get_random_node()
                self.node = node
                redis_connection = self.cluster.get_redis_connection(node)
                self.connection_pool = redis_connection.connection_pool
            self.connection = self.connection_pool.get_connection(
                'pubsub',
                self.shard_hint
            )
            # register a callback that re-subscribes to any channels we
            # were listening to when we were disconnected
            self.connection.register_connect_callback(self.on_connect)
        connection = self.connection
        self._execute(connection, connection.send_command, *args)

    def get_redis_connection(self):
        """
        Get the Redis connection of the pubsub connected node.
        """
        if self.node is not None:
            return self.node.redis_connection

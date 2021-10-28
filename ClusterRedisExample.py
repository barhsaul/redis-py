from redis import RedisCluster as Redis
from redis.cluster import ClusterNode

host = 'localhost'
startup_nodes = [ClusterNode(host, 16379), ClusterNode(host, 16380)]

# from_url examples
rc_url = Redis.from_url("redis://localhost:16379/0")
print(rc_url.cluster_slots())
print(rc_url.ping(Redis.PRIMARIES))
print(rc_url.ping(Redis.REPLICAS))
print(rc_url.ping(Redis.RANDOM))
print(rc_url.ping(Redis.ALL_NODES))
print(rc_url.execute_command("STRALGO", "LCS", "STRINGS", "string1",
                             "string2",
                             target_nodes=rc_url.get_random_node()))
print(rc_url.client_list())
print(rc_url.set('foo', 'bar1'))
print(rc_url.mget('{bar}1', '{bar}2'))
print(rc_url.set('zzzsdfsdf', 'bar2'))
print(rc_url.keyslot('bar'))
print(rc_url.set('{000}', 'bar3'))
print(f"get_nodes: {rc_url.get_nodes()}")
print(rc_url.get('foo'))
print(rc_url.keys())
# rc = Redis(host=host, port=6379)
rc = Redis(startup_nodes=startup_nodes, decode_responses=True)
print(rc.get('{000}'))
print(rc.keys())
print(rc.cluster_save_config(rc.get_primaries()))
print(rc.cluster_save_config(rc.get_node(host=host, port=16379)))

# READONLY examples
rc_readonly = Redis(startup_nodes=startup_nodes, read_from_replicas=True,
                    debug=True)
rc_readonly.set('bar', 'foo')
for i in range(0, 4):
    # Assigning the read command to the slot's servers in a Round-Robin manner
    print(rc_readonly.get('bar'))
# set command would be directed only to the slot's primary node
# reset READONLY flag
print(rc_readonly.readwrite())
for i in range(0, 4):
    # now the get command would be directed only to the slot's primary node
    print(rc_readonly.get('bar'))

from redis import RedisCluster as Redis
from redis.cluster import ClusterNode

host = 'localhost'
startup_nodes = [ClusterNode(host, 6378), ClusterNode(host, 6379)]

# rc = Redis(host=host, port=6379)
rc = Redis(startup_nodes=startup_nodes)
print(rc.get('foo'))
print(rc.keys())
print(rc.cluster_save_config(rc.get_all_primaries()))
print(rc.cluster_save_config(rc.get_node(host=host, port=6378)))

# READONLY examples
rc_readonly = Redis(startup_nodes=startup_nodes, read_from_replicas=True)
for i in range(0, 3):
    # Assigning the read command to the slot's servers in a Round-Robin manner
    print(rc_readonly.get('bar'))
# set command would be directed only to the slot's primary node
rc_readonly.set('bar', 'foo2')
# reset READONLY flag
print(rc_readonly.readwrite())
# now the get command would be directed only to the slot's primary node
print(rc_readonly.get('bar'))

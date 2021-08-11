from redis import RedisCluster as Redis
from redis.cluster import ClusterNode

host = 'localhost'
startup_nodes = [ClusterNode(host, 6378), ClusterNode(host, 6379)]

#rc = Redis(host=host, port=6379)
rc = Redis(startup_nodes=startup_nodes)
print(rc.get('foo'))
print(rc.keys())
print(rc.cluster_save_config(rc.get_all_primaries()))
print(rc.cluster_save_config(rc.get_node(host=host, port=6378)))

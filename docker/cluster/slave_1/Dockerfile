FROM redis-py-base:latest

COPY redis.conf /usr/local/etc/redis/redis.conf
COPY nodes.conf /usr/local/etc/redis/nodes.conf

EXPOSE 7002

RUN chmod 666 /usr/local/etc/redis/nodes.conf
CMD [ "redis-server", "/usr/local/etc/redis/redis.conf" ]

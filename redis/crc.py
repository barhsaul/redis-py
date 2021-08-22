from binascii import crc_hqx

__all__ = [
    "crc16",
    "key_slot",
]


def crc16(data):
    return crc_hqx(data, 0)


def key_slot(key, bucket):
    """Calculate key slot for a given key.
    :param key - bytes
    :param bucket - int
    """
    start = key.find(b"{")
    if start > -1:
        end = key.find(b"}", start + 1)
        if end > -1 and end != start + 1:
            k = key[start + 1: end]
    return crc16(key) % bucket

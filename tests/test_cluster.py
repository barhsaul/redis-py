from .conftest import skip_if_not_cluster_mode


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

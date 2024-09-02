import pytest

from improv.store import StoreInterface, RedisStoreInterface

from scipy.sparse import csc_matrix
import numpy as np
import redis
import logging

from improv.store import CannotConnectToStoreInterfaceError

WAIT_TIMEOUT = 10

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def test_connect(setup_store, server_port_num):
    store = StoreInterface(server_port_num=server_port_num)
    assert isinstance(store.client, redis.Redis)


def test_redis_connect(setup_store, server_port_num):
    store = RedisStoreInterface(server_port_num=server_port_num)
    assert isinstance(store.client, redis.Redis)
    assert store.client.ping()


def test_redis_connect_wrong_port(setup_store, server_port_num):
    bad_port_num = 1234
    with pytest.raises(CannotConnectToStoreInterfaceError) as e:
        RedisStoreInterface(server_port_num=bad_port_num)
    assert e.value.message == "Cannot connect to store at {}".format(str(bad_port_num))


# TODO: @pytest.parameterize...store.get and store.getID for diff datatypes,
# pickleable and not, etc.
# Check raises...CannotGetObjectError (object never stored)
def test_init_empty(setup_store, server_port_num):
    store = StoreInterface(server_port_num=server_port_num)
    # logger.info(store.client.config_get())
    assert store.get_all() == []


def test_is_csc_matrix_and_put(setup_store, server_port_num):
    mat = csc_matrix((3, 4), dtype=np.int8)
    store = StoreInterface(server_port_num=server_port_num)
    x = store.put(mat)
    assert isinstance(store.get(x), csc_matrix)


@pytest.mark.skip
def test_get_list_and_all(setup_store, server_port_num):
    store = StoreInterface(server_port_num=server_port_num)
    # id = store.put(1, "one")
    # id2 = store.put(2, "two")
    # id3 = store.put(3, "three")
    assert [1, 2] == store.getList(["one", "two"])
    assert [1, 2, 3] == store.get_all()


def test_reset(setup_store, server_port_num):
    store = StoreInterface(server_port_num=server_port_num)
    store.reset()
    id = store.put(1)
    assert store.get(id) == 1


def test_put_one(setup_store, server_port_num):
    store = StoreInterface(server_port_num=server_port_num)
    id = store.put(1)
    assert 1 == store.get(id)


def test_redis_put_one(setup_store, server_port_num):
    store = RedisStoreInterface(server_port_num=server_port_num)
    key = store.put(1)
    assert 1 == store.get(key)


@pytest.mark.skip(reason="Error not being raised")
def test_put_twice(setup_store):
    # store = StoreInterface()
    with pytest.raises(PlasmaObjectExists) as e:
        # id = store.put(2, "two")
        # id2 = store.put(2, "two")
        pass
        # Check that the exception thrown is an PlasmaObjectExists
    assert e.value.message == "Object already exists. Meant to call replace?"


def test_getOne(setup_store, server_port_num):
    store = StoreInterface(server_port_num=server_port_num)
    id = store.put(1)
    assert 1 == store.get(id)


def test_redis_get_one(setup_store, server_port_num):
    store = RedisStoreInterface(server_port_num=server_port_num)
    key = store.put(3)
    assert 3 == store.get(key)

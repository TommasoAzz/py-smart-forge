import logging
import threading
from enum import Enum, unique
from typing import List, Union, Dict

from redis import Redis

_ValueType = Union[bytes, float, int, bool, str, None]
_ValueTypeNotNone = Union[bytes, float, int, bool, str]

"""
Logger
"""
logger = logging.getLogger("RedisConnector")
logger.setLevel(level=logging.INFO)


@unique
class RedisType(Enum):
    string = 0
    byte = 1
    integer = 2
    float = 3
    boolean = 4


class RedisConnector:
    """
    Connector for Redis server instances.
    Support for SET, GET, MSET, MGET operations.
    """
    def __init__(self, host: str, port: int, password: str = "") -> None:
        logger.info(f"Creating a new RedisConnector connecting to {host}:{port}.")
        self._host = host
        self._port = port
        self._password = password
        self._conn = None
        self._connected = False
        self._lock = threading.Lock()

    def connect(self, db: int = 0):
        """
        Opens a connection to the Redis instance (parameters specified when building the object).
        The connection is open to database number ```db```.
        """
        self._lock.acquire()
        self._conn = Redis(self._host, self._port, db, self._password)
        self._connected = True
        self._lock.release()

    def disconnect(self):
        """
        Closes the connection to the Redis instance.
        """
        self._lock.acquire()
        del self._conn
        self._connected = False
        self._lock.release()

    @property
    def is_connected(self) -> bool:
        """
        Returns ```true``` if the object is connected to the Redis instance,
        ```false``` otherwise.
        """
        self._lock.acquire()
        conn = self._connected
        self._lock.release()

        return conn

    def set(self, key: str, val: _ValueTypeNotNone) -> None:
        """
        Sets one key-value pair in which the key is ```key``` (a string) and
        the value is ```val``` which can be a number, a boolean, a string or bytes.
        Boolean values get converted to strings.
        """
        self._lock.acquire()
        if not self._connected:
            logger.error("Not connected to Redis.")
            self._lock.release()
            return
        
        if val is True or val is False:  # <=> type(val) == bool
            self._conn.set(key, str(val))
        else:
            self._conn.set(key, val)
        self._lock.release()

    def multiple_set(self, pairs: Dict[str, _ValueTypeNotNone]) -> None:
        """
        Sets many key-value pairs in which the keys are ```pairs.keys()``` (strings) and
        the values are ```pairs.values()``` which can be a numbers, booleans, strings or bytes.
        Boolean values get converted to strings (side-effect).
        """
        self._lock.acquire()
        if not self._connected:
            logger.error("Not connected to Redis.")
            self._lock.release()
            return

        # Creating a dictionary of the key value pairs
        for key, val in pairs.items():
            if val is True or val is False:  # <=> type(val) == bool
                pairs[key] = str(val)
        
        self._conn.mset(pairs)
        self._lock.release()

    @staticmethod
    def __convert_values(values: List[Union[bytes, None]], data_types: List[RedisType]) -> List[_ValueType]:
        """
        Converts a list of ```values``` following (in order) the types inside ```data_types```.
        """
        ret: List[Union[bytes, float, int, bool, str, None]] = list()

        for value, data_type in zip(values, data_types):
            if value is not None and data_type is not RedisType.byte:
                decoded = value.decode("utf-8")
                # if data_type is RedisType.string we're done.
                if data_type is RedisType.integer:
                    decoded = int(decoded)
                elif data_type is RedisType.float:
                    decoded = float(decoded)
                elif data_type is RedisType.boolean:
                    decoded = eval(decoded)
                ret.append(decoded)
            else:
                ret.append(value)

        return ret

    def get(self, key: str, data_type: RedisType) -> _ValueType:
        """
        Retrieves the value associated to ```key``` in the active Redis instance,
        converting to type ```data_type``` before returning it.
        ```None``` is returned if the connection to Redis is not established.
        """
        self._lock.acquire()
        if not self._connected:
            logger.error("Not connected to Redis.")
            self._lock.release()
            return None

        ret = RedisConnector.__convert_values([self._conn.get(key)], [data_type])
        self._lock.release()

        return ret[0]

    def multiple_get(self, keys: List[str], data_types: List[RedisType]) -> List[_ValueType]:
        """
        Retrieves the values associated to ```keys``` in the active Redis instance,
        converting to types ```data_types``` before returning them.
        ```list()``` (or ```[]```, i.e., an empty list) is returned if the connection 
        to Redis is not established.
        """
        self._lock.acquire()
        if not self._connected:
            logger.error("Not connected to Redis.")
            self._lock.release()
            return list()

        if len(keys) != len(data_types):
            logger.error("Arguments keys and data_types do not have the same number of items.")
            self._lock.release()
            return list()

        values = self._conn.mget(keys) if len(keys) > 1 else [self._conn.get(keys[0])]

        ret = RedisConnector.__convert_values(values, data_types)

        self._lock.release()
        return ret

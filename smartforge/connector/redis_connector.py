import logging
import threading
from enum import Enum, unique
from typing import List, Union

from redis import Redis


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
    def __init__(self, host: str, port: int, password: str = "") -> None:
        logger.info(f"Creating a new RedisConnector connecting to {host}:{port}.")
        self._host = host
        self._port = port
        self._password = password
        self._connected = False
        self._lock = threading.Lock()

    def connect(self, db: int = 0):
        self._lock.acquire()
        self._conn = Redis(self._host, self._port, db, self._password)
        self._connected = True
        self._lock.release()

    def disconnect(self):
        self._lock.acquire()
        del self._conn
        self._connected = False
        self._lock.release()

    @property
    def is_connected(self) -> bool:
        self._lock.acquire()
        conn = self._connected
        self._lock.release()

        return conn

    def set(self, key: str, val: Union[bytes, float, int, bool, str]) -> None:
        self._lock.acquire()
        if not self._connected:
            logger.error("Not connected to Redis.")
            self._lock.release()
            return
        
        if type(val) == bool:
            self._conn.set(key, str(val))
        else:
            self._conn.set(key, val)
        self._lock.release()

    def multiple_set(self, keys: List[str], vals: List[Union[bytes, float, int, bool, str]]) -> None:
        self._lock.acquire()
        if not self._connected:
            logger.error("Not connected to Redis.")
            self._lock.release()
            return

        if len(keys) != len(vals):
            logger.error("Arguments keys and vals do not have the same number of items.")
            self._lock.release()
            return

        if len(keys) == 1:
            self._conn.set(keys[0], str(vals[0]) if type(vals[0]) == bool else vals[0])
            self._lock.release()
            return

        to_set = {
            k: v for k, v in zip(keys, vals) if type(v) == bool
        } | {
            k: v for k, v in zip(keys, vals) if type(v) != bool
        }
        self._conn.mset(to_set)
        self._lock.release()

    @staticmethod
    def __convert_value(values: List[Union[bytes, None]], data_types: List[RedisType]) -> List[Union[bytes, float, int, bool, str, None]]:
        ret: List[Union[bytes, float, int, bool, str, None]] = list()

        for value, data_type in zip(values, data_types):
            if value is None or data_type is RedisType.byte:
                ret.append(value)

            ret = value.decode("utf-8")
            if data_type is RedisType.integer:
                ret = int(ret)
            elif data_type is RedisType.float:
                ret = float(ret)
            elif data_type is RedisType.boolean:
                ret = eval(ret)
            
            ret.append(value)

        return ret

    def get(self, key: str, data_type: RedisType = RedisType.string) -> Union[bytes, float, int, bool, str, None]:
        self._lock.acquire()
        if not self._connected:
            logger.error("Not connected to Redis.")
            self._lock.release()
            return None

        ret = RedisConnector.__convert_value([self._conn.get(key)], [data_type])
        self._lock.release()

        return ret

    def multiple_get(self, keys: List[str], data_types: List[RedisType]) -> List[Union[bytes, float, int, bool, str, None]]:
        self._lock.acquire()
        if not self._connected:
            logger.error("Not connected to Redis.")
            self._lock.release()
            return None

        if len(keys) != len(data_types):
            logger.error("Arguments keys and data_types do not have the same number of items.")
            self._lock.release()
            return

        if len(keys) == 1:
            ret = RedisConnector.__convert_value(self._conn.get(keys[0]), data_types[0])
            self._lock.release()
            return ret

        ret = RedisConnector.__convert_value(self._conn.mget(keys), data_types)

        self._lock.release()
        return ret

from typing import List

from smartforge.connector import RedisConnector


class NotConnectedException(Exception):
    def __str__(self) -> str:
        return "NotConnectedException: A ConfigManager " \
            "can be utilized only if the connector " \
            "given as input is connected to a Redis instance."


class IncompleteConfigException(Exception):
    def __str__(self) -> str:
        return "IncompleteConfigException: The loaded " \
            "configuration does not contain all the expected " \
            "configurable properties."


__expected_keys = [
    "hosts", "endpoints", "opc_tags"
]


class ConfigManager:
    def __init__(self, connector: RedisConnector):
        self._conn = connector
        if not self._conn.is_connected:
            raise NotConnectedException()

        self._keys = self._conn.keys()
        for expected in __expected_keys:
            if expected not in self._keys:
                raise IncompleteConfigException()
        
        self._config = {k: self._conn.json_get(k) for k in self._keys}

    @property
    def configurable_properties(self) -> List[str]:
        return self._keys

    @property
    def configuration(self) -> dict:
        return self._config

    @property
    def hosts(self) -> dict:
        return self._config["hosts"]

    @property
    def endpoints(self) -> dict:
        return self._config["endpoints"]

    @property
    def opc_tags(self) -> dict:
        return self._config["opc_tags"]
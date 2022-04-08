import logging
import threading
from typing import Union

from asyncua import Client
from asyncua import ua

"""
Logger
"""
logger = logging.getLogger("OPCUAConnector")
logger.setLevel(level=logging.INFO)


class OPCUAConnector:
    """
    Connector for OPC-UA servers.
    """

    def __init__(self,
                 host: str,
                 username: str = "",
                 password: str = "",
                 security: str = "",
                 timeout: float = 1.0) -> None:
        """
        The host requires both the hostname and port to be specified.
        """
        logger.info(f"Creating a new OPCUAConnector connecting to {host}.")
        self._host = host
        self._username = username
        self._password = password
        self._security = security
        self._client = Client(f"opc.tcp://{self._host}", timeout)
        self._client.set_user(self._username)
        self._client.set_password(self._password)
        self._connected = False
        self._lock = threading.Lock()

    async def connect(self) -> None:
        self._lock.acquire()
        await self._client.set_security_string(self._security)
        await self._client.connect()
        self._connected = True
        self._lock.release()

    async def disconnect(self) -> None:
        self._lock.acquire()
        await self._client.disconnect()
        self._connected = False
        self._lock.release()

    @property
    def is_connected(self) -> bool:
        self._lock.acquire()
        conn = self._connected
        self._lock.release()

        return conn

    async def set(self,
                  node_id: str,
                  value: Union[bool, float, int],
                  precision: Union[int, None] = None) -> None:
        variant_type: Union[ua.VariantType, None] = None

        if type(value) == bool:
            variant_type = ua.VariantType.Boolean
        elif type(value) == float:
            if precision == 32:
                variant_type = ua.VariantType.Float
            elif precision == 64 or precision is None:
                variant_type = ua.VariantType.Double
        elif type(value) == int:
            if precision == 16:
                variant_type = ua.VariantType.Int16
            elif precision == 32:
                variant_type = ua.VariantType.Int32
            elif precision == 64 or precision is None:
                variant_type = ua.VariantType.Int64

        if variant_type is None:
            logger.error("value has a wrong type, "
                         "it can only have types: bool, float, int")
            return

        self._lock.acquire()
        node = self._client.get_node(node_id)
        to_write = ua.Variant(value, variant_type)
        await node.write_attribute(ua.AttributeIds.Value, ua.DataValue(to_write))
        self._lock.release()

    async def get(self, node_id: str) -> Union[bool, float, int]:
        self._lock.acquire()
        node = self._client.get_node(node_id)
        attr = await node.read_attribute(ua.AttributeIds.Value)
        self._lock.release()

        return attr.Value.Value

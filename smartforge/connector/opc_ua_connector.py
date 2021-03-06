from __future__ import annotations
import threading
from typing import Union, Dict
from abc import ABC, abstractmethod

from asyncua import Client, Node, ua
from asyncua.common.subscription import Subscription
from ..utils import get_logger

"""
Logger
"""
logger = get_logger("OPCUAConnector")


class OPCUAConnector:
    """
    Connector for OPC-UA servers.
    """

    def __init__(self,
                 host: str,
                 username: str = "",
                 password: str = "",
                 security: str = "",
                 timeout: float = 1.0,
                 lock_protection: bool = False) -> None:
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
        self._subscriptions: Dict[str, Subscription] = {}
        self._connected = False
        self._lock = threading.Lock()
        self._lock = threading.Lock()
        if lock_protection:
            self._acquire = lambda: self._lock.acquire()
            self._release = self._lock.release
        else:
            self._acquire = lambda: True
            self._release = lambda: None

    async def connect(self) -> None:
        try:
            self._acquire()
            if self._connected:
                logger.warning("Already connected to OPC-UA.")
                self._release()
                return
            
            await self._client.set_security_string(self._security)
            await self._client.connect()
            self._connected = True
        except Exception as exc:
            logger.error(f"{type(exc)} while connecting.")
            raise
        finally:
            self._release()

    async def disconnect(self) -> None:
        try:
            self._acquire()
            if not self._connected:
                logger.warning("Not connected to OPC-UA.")
                self._release()
                return

            if len(self._subscriptions) > 0:
                logger.warning(f"Removing subscriptions for tags: {self._subscriptions.keys()}")
                for subscription in self._subscriptions.values():
                    await subscription.delete()

            await self._client.disconnect()
            self._connected = False
        except Exception as exc:
            logger.error(f"{type(exc)} while disconnecting.")
            raise
        finally:
            self._release()

    @property
    def is_connected(self) -> bool:
        self._acquire()
        conn = self._connected
        self._release()

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

        self._acquire()
        if not self._connected:
            logger.error("Not connected to OPC-UA.")
            self._release()
            return

        node = self._client.get_node(node_id)
        to_write = ua.Variant(value, variant_type)
        await node.write_attribute(ua.AttributeIds.Value, ua.DataValue(to_write))
        self._release()

    async def get(self, node_id: str) -> Union[bool, float, int]:
        self._acquire()
        if not self._connected:
            logger.error("Not connected to OPC-UA.")
            self._release()
            return None

        node = self._client.get_node(node_id)
        attr = await node.read_attribute(ua.AttributeIds.Value)
        self._release()

        return attr.Value.Value

    async def start_subscription(self,
                                 node_id: str,
                                 subscription_handler: OPCUASubscriptionHandler,
                                 update_interval: int = 500) -> None:
        self._acquire()
        if not self._connected:
            logger.error("Not connected to OPC-UA.")
            self._release()
            return None

        subscription: Subscription = await self._client.create_subscription(update_interval, subscription_handler)
        self._subscriptions[node_id] = subscription
        node = self._client.get_node(node_id)
        await subscription.subscribe_data_change(node)
        self._release()

    async def stop_subscription(self, node_id: str):
        self._acquire()
        if not self._connected:
            logger.error("Not connected to OPC-UA.")
            self._release()
            return None

        subscription = self._subscriptions.get(node_id)
        if subscription is not None:
            await subscription.delete()
            self._subscriptions.pop(node_id)

        self._release()


class OPCUASubscriptionHandler(ABC):
    @abstractmethod
    def datachange_notification(self, node: Node, val, data) -> None:
        pass

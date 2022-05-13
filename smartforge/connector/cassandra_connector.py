import threading
from cassandra.cluster import Cluster
from cassandra.cluster import Session
from cassandra import ProtocolVersion
from cassandra.auth import PlainTextAuthProvider

from ..utils import get_logger

"""
Logger
"""
logger = get_logger("CassandraConnector")


class CassandraConnector:
    def __init__(self, host: str, port: int, username: str = "", password: str = "", lock_protection: bool = False) -> None:
        logger.info(f"Creating a new CassandraConnector connecting to {host}:{port}.")
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._cluster = None
        self._session = None
        self._keyspace = None
        self._connected = False
        self._lock = threading.Lock()
        if lock_protection:
            self._acquire = lambda: self._lock.acquire()
            self._release = self._lock.release
        else:
            self._acquire = lambda: True
            self._release = lambda: None

    def connect(self, keyspace: str):
        self._acquire()
        if self._connected:
            logger.warning("Already connected to Cassandra")
            self._release()
            return
        
        self._keyspace = keyspace
        if self._username == "" and self._password == "":
            logger.warning("No username or password are set. If this is what you expect, ignore the message.")
            auth_provider = None
        else:
            auth_provider = PlainTextAuthProvider(
                username=self._username,
                password=self._password
            )
        self._cluster = Cluster(
            [self._host],
            port=self._port,
            protocol_version=ProtocolVersion.V5,
            auth_provider=auth_provider)
        self._session: Session = self._cluster.connect(keyspace)
        self._connected = True
        self._release()

    def disconnect(self):
        self._acquire()
        if not self._connected:
            logger.warning("Not connected to Cassandra")
            self._release()
            return
        
        self._cluster.shutdown()
        self._connected = False
        self._release()
    
    @property
    def is_connected(self) -> bool:
        self._acquire()
        conn = self._connected
        self._release()
        
        return conn

    def insert(self, statement: str, args: dict = {}):
        self._acquire()
        if not self._connected:
            logger.error("Not connected to Cassandra")
            self._release()
            return
        
        # Using self._session.execute_async since these are INSERT queries, hence nothing is returned.
        if "%" in statement and len(args) > 0:
            self._session.execute(statement, args)
        elif "%" in statement and len(args) == 0:
            # Not executing queries, since it will result in an error!
            logger.warn(
                f"Statement: {statement} expects arguments that were not given.")
        # elif "%" not in statement and len(args) > 0:  # not interesting
        else:
            self._session.execute(statement)
        self._release()

    def register_type(self, cassandra_type: str, user_type) -> None:
        self._acquire()
        if not self._connected:
            logger.error("Not connected to Cassandra")
            self._release()
            return

        self._cluster.register_user_type(self._keyspace, cassandra_type, user_type)

        self._release()

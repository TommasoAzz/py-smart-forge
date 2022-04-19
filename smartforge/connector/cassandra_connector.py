import logging
import threading
from cassandra.cluster import Cluster
from cassandra.cluster import Session
from cassandra import ProtocolVersion
from cassandra.auth import PlainTextAuthProvider

"""
Logger
"""
logger = logging.getLogger("CassandraConnector")
logger.setLevel(level=logging.INFO)


class CassandraConnector:
    def __init__(self, host: str, port: int, username: str = "", password: str = "") -> None:
        logger.info(f"Creating a new CassandraConnector connecting to {host}:{port}.")
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        if username == "" and password == "":
            logger.warn("No username or password are set. If this is what you expect, ignore the message.")
            auth_provider = None
        else:
            auth_provider = PlainTextAuthProvider(
                username=username,
                password=password
            )
        self._cluster = Cluster(
            [host],
            port=port,
            protocol_version=ProtocolVersion.V5,
            auth_provider=auth_provider)
        self._session = None
        self._keyspace = None
        self._connected = False
        self._lock = threading.Lock()

    def connect(self, keyspace: str):
        self._lock.acquire()
        self._keyspace = keyspace
        self._session: Session = self._cluster.connect(keyspace)
        self._connected = True
        self._lock.release()

    def disconnect(self):
        self._lock.acquire()
        self._cluster.shutdown()
        self._connected = False
        self._lock.release()
    
    @property
    def is_connected(self) -> bool:
        self._lock.acquire()
        conn = self._connected
        self._lock.release()
        
        return conn

    def insert(self, statement: str, args: dict = {}):
        self._lock.acquire()
        if not self._connected:
            self._lock.release()
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
        self._lock.release()

    def register_type(self, cassandra_type: str, user_type) -> None:
        self._lock.acquire()
        if not self._connected:
            self._lock.release()
            return

        self._cluster.register_user_type(self._keyspace, cassandra_type, user_type)

        self._lock.release()
**Version 0.3.5**

- Added support for more types in RedisConnector json_set
- Removed support for bytes in RedisConnector json_get (it is actually not supported)
- Improved error messages while (dis)connecting to an OPC-UA server in OPCUAConnector and added rethrowing of exceptions

**Version 0.3.4**

- Added support for RedisJSON module for Redis

**Version 0.3.3**

- Allowed change of subscription time in OPCUAConnector

**Version 0.3.2**

- Fixed setup.py

**Version 0.3.1**

- Fixed connection to cluster in CassandraConnector

**Version 0.3.0**

- Added possibility to register types to CassandraConnector
- Updated Redis dependency

**Version 0.2.0**

- Added subscription to tags feature to OPCUAConnector
- Fixed OPCUAConnector, now checking connection before set, get, start_subscription, stop_subscription

**Version 0.1.2**

- Fixed RedisConnector
- Added docs to RedisConnector

**Version 0.1.1**

- Fixed setup.py
- Added CHANGES

**Version 0.1.0**
Initial release with the following features:

- connector for Cassandra
- connector for Redis Strings (single and batch)
- connector for OPC-UA server
- connector for Smart Forge Simulator (default_parameters only)
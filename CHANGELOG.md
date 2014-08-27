CassandraDB Connector Release Notes
==========================================

Date: 30-May-2014

Version: 1.2.1

Supported API versions: Cassandra Thrift API v1.0 - http://wiki.apache.org/cassandra/API10

Supported Mule Runtime Versions: 3.5.x

Closed Issues in this release
------------------------------

 - Added functional automation test cases.
 - Upgraded Mule Devkit to 3.5.1.

 - The list of operations supported by this version of the connector are
    - Set QueryKeyspace
    - Get
    - Get Row
    - Get Slice
    - Multi Get Slice
    - Get Count
    - Multi Get Count
    - Get Range Slices
    - Get Indexed Slices
    - Insert
    - Insert from Map
    - Batch Mutable
    - Add
    - Remove
    - Remove Counter
    - Truncate
    - Describe Cluster Name
    - Describe Schema Versions
    - Describe Keyspace
    - Describe Partitioner
    - Describe Ring
    - Describe Snitch
    - Describe Version
    - System Add Column Family from Object
    - System Add Column Family from Object With Simple Names
    - System Add Column Family With Params
    - System Drop Column Family
    - System Add Keyspace From Object
    - System Add Keyspace With Params
    - System Drop Keyspace
    - System Update Keyspace
    - System Update Column Family
    - System Execute CQL Query

Known Issues in this release
------------------------------

 - NA -

1.2.0
=====

 - Upgraded Mule Devkit to 3.5.0.
 - Removed @Optional as it is redundant when used along @Default.

1.1.1
=====

 - Implemented on Mule Devkit version 3.4.1.
 - Initial Version.
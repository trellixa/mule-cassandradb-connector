Apache Cassandra Connector Demo
====================================
Mule Studio demo for Apache Cassandra connector.


Prerequisites
---------------

A running instance of Apache Cassandra with a Keyspace already created.

How to Run Sample
-----------------

1. Import the project folder demo in Studio.
2. Update the Apache Cassandra connection parameters in /src/main/app/mule-app.properties.
3. From the 'Global Elements' tab, open the CassandraDB: Username/Password Connection.
4. Click on 'Test Connection' to make sure the connection works correctly.
5. Run the application.

About the Sample
----------------

Using a browser, navigate to http://localhost:8081/.
When navigating to this page, a table called DEMO(id - int PK, name - text, event - text) is created in the keyspace specified in the Connection parameters. This table will be used for some of the operations.

The operations presented in the demo:

* Create table - Creates a new table in the specified keyspace. If no keyspace is specified for the operation the table will be created in the keyspace present in the connection parameters.
* Drop table - Drops a table from the specified keyspace. If no keyspace is specified for the operation the table will be created in the keyspace present in the connection parameters.
* Get tables from keyspace - Returns all table names form the specified keyspace. If no keyspace is specified for the operation the table will be created in the keyspace present in the connection parameters.
* Alter Table operations(Add new column, Remove column, Rename column, Change column type) - These operations perform structure changes on tables from the keyspace specified in the connection parameters.
* Insert - Inserts objects into the DEMO table.
* Select - Selects all objects from the DEMO table
* Update - Updates the Name or the Event for the objects with the specified Id in the DEMO table
* Delete rows - Deletes objects from the DEMO table by the specified ids
* Delete columns - Deletes certain values(Name, Event) for the specified ids in the DEMO table
* Execute CQL query - Executes the given query(SELECT * FROM DEMO WHERE id IN (?,?)) with the given parameters

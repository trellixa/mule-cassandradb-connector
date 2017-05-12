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
When running the demo a default keyspace(demo_keyspace) and a default table(demo_table(id-INT,name-TEXT,event-TEXT)) in this keyspace are created in order to be further used by some operations.

The operations presented in the demo:

* Create keyspace - Creates a new keyspace. This keyspace can be used for other operations.
* Drop keyspace - Deletes a keyspace along with its data.
* Create table - Creates a new table in the specified keyspace. If no keyspace is specified for the operation the table will be created in the keyspace present in the connection parameters.
* Drop table - Drops a table from the specified keyspace. If no keyspace is specified for the operation the table will be created in the keyspace present in the connection parameters.
* Get tables from keyspace - Returns all table names form the specified keyspace. If no keyspace is specified for the operation the table will be created in the keyspace present in the connection parameters.
* Alter Table operations(Add new column, Remove column, Rename column, Change column type) - These operations perform structure changes on tables from the specified keyspace or in the keyspace specified in the connection parameters.
* Insert - Inserts objects into the DEMO table.
* Select - Selects all objects from the DEMO table
* Update - Updates the Name or the Event for the objects with the specified Id in the DEMO table
* Delete rows - Deletes objects from the DEMO table by the specified ids
* Delete columns - Deletes certain values(Name, Event) for the specified ids in the DEMO table
* Execute CQL query - Executes the given query(SELECT * FROM DEMO WHERE id IN (?,?)) with the given parameters. Also, other queries can be executed using this operation.

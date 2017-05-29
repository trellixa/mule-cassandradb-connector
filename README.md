## Anypoint Connector for Cassandra Database

Apache Cassandra is a massively scalable open source non-relational database that offers continuous availability, linear 
scale performance, operational simplicity and easy data distribution across multiple data centers and cloud availability zones.
Cassandra was originally developed at Facebook, was open sourced in 2008, and became a top-level Apache project in 2010.

The connector exposes Data manipulation && Schema manipulation operations.

## Author
 
MuleSoft Inc.

## Supported Mule runtime versions

Mule 3.6+

## Installation 

* Apache Cassandra is a open source software and can be installed following the instructions here http://cassandra.apache.org/download/
* After finishing the installation find the cassandra.yaml config file and change the value of the authenticator from AllowAllAuthenticator to PasswordAuthenticator
* After starting the cassandra service, you can connect to the database that will be running at port 9042 by default; the default credentials are cassandra/cassandra but also other admin accounts can be created
* Install the connector in Anypoint Studio; all setup is now finished.
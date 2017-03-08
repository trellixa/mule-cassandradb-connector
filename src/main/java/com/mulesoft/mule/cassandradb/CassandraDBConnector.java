/**
 * Mule Cassandra Connector
 * <p>
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * <p>
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.mulesoft.mule.cassandradb;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.schemabuilder.CreateKeyspace;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import org.mule.api.ConnectionException;
import org.mule.api.annotations.*;
import org.mule.api.annotations.display.Password;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Default;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Apache Cassandra database is the right choice when you need scalability and high availability without compromising performance.
 * Cassandra's ColumnFamily data model offers the convenience of column indexes with the performance of log-structured updates, strong support for materialized views, and powerful built-in caching.
 *
 * @author MuleSoft, Inc.
 */
@Connector(name = "cassandradb", schemaVersion = "3.2", friendlyName = "CassandraDB", minMuleVersion = "3.5")
public class CassandraDBConnector {
    private static final Logger logger = LoggerFactory.getLogger(CassandraDBConnector.class);

    /**
     * Host name or IP address
     */
    @Configurable
    @Default("localhost")
    private String host;

    /**
     * Port (default is 9160)
     */
    @Configurable
    @Default("9160")
    private int port = 9160;

    /**
     * Cassandra keyspace
     */
    @Configurable
    private String keyspace;

    /**
     * Cassandra client
     * session to be used to execute queries
     */
    private CassandraClient cassandraClient;

    /**
     * Method invoked when a connection is required
     *
     * @param username A username. NOTE: Please use a dummy username if you have disabled authentication
     * @param password A password. NOTE: Leave empty if not required. If specified, the connector will try to login with this credentials
     * @throws org.mule.api.ConnectionException
     */
    @Connect
    public void connect(@ConnectionKey String username,
                        @Password String password)
            throws ConnectionException {
        cassandraClient.connect(host, port, username, password, keyspace);
    }


    /**
     * Disconnect
     */
    @Disconnect
    public void disconnect() {
        if (isConnected()) {
            try {
                cassandraClient.close();
            } catch (Exception e) {
                logger.error("Exception thrown while trying to disconnect:", e);
            }
        }
    }

    /**
     * Are we connected
     *
     * @return the connection status of the connector.
     */
    @ValidateConnection
    public boolean isConnected() {
        return cassandraClient.getSession().isClosed();
    }

    /**
     * Connection Identifier
     *
     * @return the connection identifier.
     */
    @ConnectionIdentifier
    public String connectionId() {
        return "unknown";
    }


//    /**
//     * Executes a CQL (Cassandra Query Language) statement and returns a
//     * CqlResult containing the results. For more information about CQL please visit: http://cassandra.apache.org/doc/cql/CQL.html
//     * <p/>
//     * {@sample.xml ../../../doc/CassandraDB-connector.xml.sample
//     * cassandradb:execute-cql-query}
//     *
//     * @param query       CQL Statement to be executed
//     * @param compression Compression level, by default we use NONE
//     * @return CqlResult containing the results of the execution
//     * @throws com.mulesoft.mule.cassandradb.CassandraDBException Generic Exception wrapper class for Thrift Exceptions.
//     */
//    @Processor
//    public Object executeCqlQuery(String query, @Default("NONE") Compression compression)
//            throws CassandraDBException {
//        try {
//            return client.execute_cql_query(CassandraDBUtils.toByteBuffer(query),
//                    compression);
//        } catch (InvalidRequestException e) {
//            throw new CassandraDBException(e.getMessage(), e);
//        } catch (UnavailableException e) {
//            throw new CassandraDBException(e.getMessage(), e);
//        } catch (TimedOutException e) {
//            throw new CassandraDBException(e.getMessage(), e);
//        } catch (SchemaDisagreementException e) {
//            throw new CassandraDBException(e.getMessage(), e);
//        } catch (TException e) {
//            throw new CassandraDBException(e.getMessage(), e);
//        }
//    }

    /**
     * Gets information about the specified keyspace.
     * <p/>
     * {@sample.xml ../../../doc/CassandraDB-connector.xml.sample
     * cassandradb:describe-keyspace}
     *
     * @return A KsDef instance
     * @throws com.mulesoft.mule.cassandradb.CassandraDBException Generic Exception wrapper class for Thrift Exceptions.
     */
    @Processor
    public Object createKeyspace(String keySpace) throws CassandraDBException {
        try {
            //build create keyspace statement
            CreateKeyspace createKeyspace = new CreateKeyspace(keySpace);
            //execute statement
            return cassandraClient.getSession().execute(createKeyspace.with());
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    /**
     * @return the host connection url.
     */
    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Retrieves the Port.
     *
     * @return the connection port.
     */
    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Retrieves the Keyspace.
     *
     * @return the keyspace.
     */
    public String getKeyspace() {
        return this.keyspace;
    }

    /**
     * Set the keyspace
     *
     * @param keyspace set the keyspace.
     */
    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    /**
     * Retrieves the Cassandra DB consistency level.
     *
     * @return the consistency level.
     */

    public void setClient(CassandraClient client) {
        this.cassandraClient = client;
    }
}

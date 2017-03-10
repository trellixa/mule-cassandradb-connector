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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.*;
import com.datastax.driver.mapping.Result;
import com.mulesoft.mule.cassandradb.utils.*;
import com.mulesoft.mule.cassandradb.utils.builders.HelperStatements;
import org.apache.commons.lang3.StringUtils;
import org.mule.api.ConnectionException;
import org.mule.api.annotations.*;
import org.mule.api.annotations.display.Password;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

    @Processor
    public ResultSet createKeyspace(String keyspaceName, Map<String, Object> replicationStrategy) throws CassandraDBException {
        try {
            return cassandraClient.getSession().execute(HelperStatements.createKeyspaceStatement(keyspaceName, replicationStrategy).getQueryString());
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    @Processor
    public ResultSet dropKeyspace(String keyspaceName) throws CassandraDBException{
        try {
            return cassandraClient.getSession().execute(HelperStatements.dropKeyspaceStatement(keyspaceName).getQueryString());
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    /**
     * method creates a table(column family) in a specific keyspace
     * @param customKeyspaceName - if the parameter is present, we overwrite the keyspace used in the connection itself (if present)
     * @param partitionKey - mandatory field; if null, default partitionKey will be added
     */
    @Processor
    public ResultSet createTable(String tableName, @Optional String customKeyspaceName, Map<String, Object> partitionKey) throws CassandraDBException{
        try {
            return cassandraClient.getSession().execute(HelperStatements.createTable(tableName,
                    StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : keyspace , partitionKey));
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    @Processor
    public ResultSet dropTable(String tableName, @Optional String customKeyspaceName) throws CassandraDBException{
        try {
            return cassandraClient.getSession().execute(HelperStatements.dropTable(tableName,
                    StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : keyspace));
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    @Processor
    public ResultSet executeCQLQuery(String cqlQuery) {
        if (StringUtils.isNotBlank(cqlQuery)) {
            return cassandraClient.getSession().execute(cqlQuery);
        }
        return null;
    }

    public List<String> getTableNamesFromKeyspace(String keyspaceName) {

        Collection<TableMetadata> tables = cassandraClient.getCluster()
                .getMetadata().getKeyspace(keyspaceName)
                .getTables();
        ArrayList<String> tableNames = new ArrayList<String>();
        for (TableMetadata table : tables) {
            tableNames.add(table.getName());
        }
        return tableNames;
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

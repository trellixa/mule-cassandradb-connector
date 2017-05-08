/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.configurations;


import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import org.mule.api.ConnectionException;
import org.mule.api.annotations.*;
import org.mule.api.annotations.components.ConnectionManagement;
import org.mule.api.annotations.display.FriendlyName;
import org.mule.api.annotations.display.Password;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Default;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ConnectionManagement(configElementName = "config", friendlyName = "Username/Password Connection")
public class BasicAuthConnectionStrategy {

    private static final Logger logger = LoggerFactory.getLogger(BasicAuthConnectionStrategy.class);

    /**
     * Host name or IP address
     */
    @Configurable
    @org.mule.api.annotations.param.Optional
    @Default("localhost")
    private String host;

    /**
     * Port (default is 9160)
     */
    @Configurable
    @org.mule.api.annotations.param.Optional
    @Default("9042")
    private int port;

    /**
     * Cassandra keyspace
     */
    @Configurable
    @org.mule.api.annotations.param.Optional
    @Default("")
    private String keyspace;

    /**
     * Cassandra cluster name
     */
    @Configurable
    @org.mule.api.annotations.param.Optional
    @Placement(group = "Advanced Settings")
    private String clusterName;

    /**
     * Versions of the native protocol supported by the driver.
     */
    @Configurable
    @org.mule.api.annotations.param.Optional
    @Placement(group = "Advanced Settings")
    private ProtocolVersion protocolVersion;

    /**
     * The maximum time to wait for schema agreement before returning from a DDL query.
     */
    @Configurable
    @org.mule.api.annotations.param.Optional
    @Placement(group = "Advanced Settings")
    private int maxSchemaAgreementWaitSeconds;

    /**
     * The compression to use for the transport.
     */
    @Configurable
    @org.mule.api.annotations.param.Optional
    @Placement(group = "Advanced Settings")
    private ProtocolOptions.Compression compression;

    /**
     * Enables the use of SSL for the created cluster.
     */
    @Configurable
    @org.mule.api.annotations.param.Optional
    @Placement(group = "Advanced Settings")
    @Default("false")
    @FriendlyName("SSL")
    private boolean sslEnabled;

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
    @TestConnectivity
    public void connect(@ConnectionKey final String username,
                        @Password final String password) throws ConnectionException {
        cassandraClient = CassandraClient.buildCassandraClient(new ConnectionParameters(host, port, username, password, keyspace,
                new AdvancedConnectionParameters(protocolVersion, clusterName, maxSchemaAgreementWaitSeconds, compression, sslEnabled)));
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
            } finally {
                cassandraClient = null;
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
        return cassandraClient != null;
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

    /**
     * @return the host connection url.
     */
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public ProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(ProtocolVersion protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public int getMaxSchemaAgreementWaitSeconds() {
        return maxSchemaAgreementWaitSeconds;
    }

    public void setMaxSchemaAgreementWaitSeconds(int maxSchemaAgreementWaitSeconds) {
        this.maxSchemaAgreementWaitSeconds = maxSchemaAgreementWaitSeconds;
    }

    public ProtocolOptions.Compression getCompression() {
        return compression;
    }

    public void setCompression(ProtocolOptions.Compression compression) {
        this.compression = compression;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public CassandraClient getCassandraClient() {
        return cassandraClient;
    }

    public void setCassandraClient(CassandraClient cassandraClient) {
        this.cassandraClient = cassandraClient;
    }
}

package org.mule.modules.cassandradb.internal.connection;

import org.mule.connectors.commons.template.connection.ConnectorConnectionProvider;
import org.mule.modules.cassandradb.api.ProtocolCompression;
import org.mule.modules.cassandradb.api.ProtocolVersion;
import org.mule.runtime.api.connection.CachedConnectionProvider;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Password;
import org.mule.runtime.extension.api.annotation.param.display.Placement;


public class BasicAuthConnectionProvider extends ConnectorConnectionProvider<CassandraConnection> implements CachedConnectionProvider<CassandraConnection> {

    /**
     * Host name or IP address
     */
    @Parameter
    @Optional(defaultValue = "localhost")
    private String host;

    /**
     * Port (default is 9042)
     */
    @Parameter
    @Optional(defaultValue = "9042")
    private String port;

    /**
     * Cassandra keyspace
     */
    @Parameter
    @Optional(defaultValue = "")
    private String keyspace;

    /**
     * Cassandra cluster name
     */
    @Parameter
    @Optional
    @Placement(tab = "Advanced Settings")
    private String clusterName;

    /**
     * @param username
     * the username to use for authentication.
     */
    @Parameter
    @Optional(defaultValue = "")
    public String username;

    /**
     * the password to use for authentication. If the password is null or whitespaces only, the connector won't use authentication and username must be empty too.
     */
    @Parameter
    @Optional
    @Password
    public String password;

    /**
     * Version of the native protocol supported by the driver.
     */
    @Parameter
    @Optional
    @Placement(tab = "Advanced Settings")
    private ProtocolVersion protocolVersion;

    /**
     * The maximum time to wait for schema agreement before returning from a DDL query.
     */
    @Parameter
    @Optional(defaultValue = "0")
    @Placement(tab = "Advanced Settings")
    private Integer maxSchemaAgreementWaitSeconds;

    /**
     * The compression to use for the transport.
     */
    @Parameter
    @Optional
    @Placement(tab = "Advanced Settings")
    private ProtocolCompression compression;

    /**
     * Enables the use of SSL for the cluster.
     */
    @Parameter
    @Placement(tab = "Advanced Settings")
    @DisplayName("SSL")
    private boolean sslEnabled;

    @Override
    public CassandraConnection connect() throws ConnectionException {
        return CassandraConnection.build(new ConnectionParameters(host, port, username,
                password, keyspace, new AdvancedConnectionParameters(protocolVersion, clusterName, maxSchemaAgreementWaitSeconds, compression, sslEnabled)));
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
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

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public ProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(ProtocolVersion protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public Integer getMaxSchemaAgreementWaitSeconds() {
        return maxSchemaAgreementWaitSeconds;
    }

    public void setMaxSchemaAgreementWaitSeconds(Integer maxSchemaAgreementWaitSeconds) {
        this.maxSchemaAgreementWaitSeconds = maxSchemaAgreementWaitSeconds;
    }

    public ProtocolCompression getCompression() {
        return compression;
    }

    public void setCompression(ProtocolCompression compression) {
        this.compression = compression;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }
}

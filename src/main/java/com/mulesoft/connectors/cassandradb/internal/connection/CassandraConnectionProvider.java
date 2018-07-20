package com.mulesoft.connectors.cassandradb.internal.connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.Session;
import org.mule.connectors.commons.template.connection.ConnectorConnectionProvider;
import com.mulesoft.connectors.cassandradb.api.ProtocolCompression;
import com.mulesoft.connectors.cassandradb.api.ProtocolVersion;
import com.mulesoft.connectors.cassandradb.internal.exception.CassandraException;
import com.mulesoft.connectors.cassandradb.internal.util.ConnectionUtil;
import org.mule.runtime.api.connection.CachedConnectionProvider;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Password;
import org.mule.runtime.extension.api.annotation.param.display.Placement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.datastax.driver.core.ProtocolVersion.valueOf;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class CassandraConnectionProvider extends ConnectorConnectionProvider<CassandraConnection> implements CachedConnectionProvider<CassandraConnection> {

    /**
     * Host name or IP address
     */
    @Parameter
    @Optional(defaultValue = "")
    private String host;

    /**
     * Port
     */
    @Parameter
    @Optional(defaultValue = "")
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
     * Cassandra cluster nodes(ip or host address and port separated by comma. E.g: host1:port1, host2:port2). If the port is not specified,
     * the default 9042 will be used.
     * When you specify this parameter, the host and port from general settings will be ignored.
     */
    @Parameter
    @Optional
    @Placement(tab = "Advanced Settings")
    private String clusterNodes;

    /**
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

    private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionProvider.class);

    @Override
    public CassandraConnection connect() throws ConnectionException {
        Cluster.Builder clusterBuilder = Cluster.builder();

        if (clusterNodes != null) {

            connectWithAdvancedParams(clusterBuilder);

        } else {

            connectWithBasicParams(clusterBuilder);

        }

        withCredentials(clusterBuilder);

        withClusterName(clusterBuilder);

        withMaxSchemaAgreementWaitSeconds(clusterBuilder);

        withProtocolVersion(clusterBuilder);

        withCompression(clusterBuilder);

        withSSL(clusterBuilder);

        Cluster cluster = clusterBuilder.build();

        Session session = null;

        try {
            logger.info("Connecting to Cassandra Database: {} , port: {} with clusterName: {} , protocol version {} and compression type {} ", host, port, clusterName != null ? clusterName : null, protocolVersion != null ? protocolVersion : null, compression != null ? compression : null);
            session = isNotEmpty(keyspace) ? cluster.connect(keyspace): cluster.connect();
            logger.info("Connected to Cassandra Cluster: {} !", session.getCluster().getClusterName());
        } catch (Exception cassandraException) {
            logger.error("Error while connecting to Cassandra database!", cassandraException);
            throw new CassandraException(cassandraException.getMessage());
        }
        return new CassandraConnection(cluster, session);
    }

    @Override
    public void disconnect(CassandraConnection connection){
        connection.disconnect();
    }

    private boolean validateAttribute(String attr){
        if(!isBlank(attr)){
            return true;
        } else {
            throw new IllegalArgumentException("Invalid parameter");
        }
    }

    private void connectWithBasicParams(Cluster.Builder clusterBuilder) throws ConnectionException {
        try {
            validateAttribute(host);
            validateAttribute(port);
            clusterBuilder.addContactPoint(host).withPort(Integer.parseInt(port));
        } catch (IllegalArgumentException connEx) {
            logger.error("Error while connecting to Cassandra database!", connEx);
            throw new CassandraException(connEx.getMessage());
        }
    }

    private void connectWithAdvancedParams(Cluster.Builder clusterBuilder) {
        try {
            Map<String, String> nodes = ConnectionUtil.parseClusterNodesString(clusterNodes);

            for (Map.Entry<String, String> entry : nodes.entrySet()) {
                clusterBuilder.addContactPoint(entry.getKey()).withPort(Integer.parseInt(entry.getValue()));
            }
        } catch (IllegalArgumentException connEx) {
            logger.error("Error while connecting to Cassandra database!", connEx);
            throw new CassandraException(connEx.getMessage());
        }
    }

    private void withCredentials(Cluster.Builder clusterBuilder) {
        if (isNotEmpty(username) && isNotEmpty(password)) {
            clusterBuilder.withCredentials(username, password);
        }
    }

    private void withClusterName(Cluster.Builder clusterBuilder){
        if (isNotEmpty(clusterName)) {
            clusterBuilder.withClusterName(clusterName);
        }
    }

    private void withMaxSchemaAgreementWaitSeconds(Cluster.Builder clusBuilder){
        if (maxSchemaAgreementWaitSeconds > 0){
            clusBuilder.withMaxSchemaAgreementWaitSeconds(maxSchemaAgreementWaitSeconds);
        }
    }

    private void withProtocolVersion(Cluster.Builder clusterBuilder){
        if(protocolVersion != null) {
            clusterBuilder.withProtocolVersion(valueOf(protocolVersion.name()));
        }
    }

    private void withCompression(Cluster.Builder clusterBuilder){
        if (compression != null){
            clusterBuilder.withCompression(ProtocolOptions.Compression.valueOf(compression.name()));
        }
    }

    private void withSSL(Cluster.Builder clusterBuilder){
        if(sslEnabled){
            clusterBuilder.withSSL();
        }
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

    public String getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(String clusterNodes) {
        this.clusterNodes = clusterNodes;
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

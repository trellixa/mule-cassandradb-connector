package org.mule.modules.cassandradb.internal.connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.mule.connectors.commons.template.connection.ConnectorConnectionProvider;
import org.mule.modules.cassandradb.api.ProtocolCompression;
import org.mule.modules.cassandradb.api.ProtocolVersion;
import org.mule.modules.cassandradb.internal.exception.CassandraException;
import org.mule.runtime.api.connection.CachedConnectionProvider;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Password;
import org.mule.runtime.extension.api.annotation.param.display.Placement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class CassandraConnectionProvider extends ConnectorConnectionProvider<CassandraConnection> implements CachedConnectionProvider<CassandraConnection> {
    private static final String CASSANDRA_NODE_DEFAULT_PORT = "9042";

    /**
     * Host name or IP address
     */
    @Parameter
    @Optional
    private String host;

    /**
     * Port
     */
    @Parameter
    @Optional(defaultValue = CASSANDRA_NODE_DEFAULT_PORT)
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
    private String username;

    /**
     * the password to use for authentication. If the password is null or whitespaces only, the connector won't use authentication and username must be empty too.
     */
    @Parameter
    @Optional
    @Password
    private String password;

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
        try {
            Cluster.Builder clusterBuilder = Cluster.builder();
            Stream.of(java.util.Optional.ofNullable(clusterNodes).orElseGet(() -> format("%s:%s", java.util.Optional.ofNullable(host).orElseThrow(IllegalArgumentException::new), port)).split(",")).forEach(part -> {
                String[] pair = part.contains(":") ? part.split(":") : new String[]{part.trim(), CASSANDRA_NODE_DEFAULT_PORT};
                clusterBuilder.addContactPoint(pair[0].trim()).withPort(Integer.parseInt(pair[1].trim()));
            });
            java.util.Optional.ofNullable(username)
                    .ifPresent(username -> java.util.Optional.ofNullable(password)
                            .ifPresent(password -> clusterBuilder.withCredentials(username, password)));
            java.util.Optional.ofNullable(clusterName)
                    .filter(Predicate.isEqual("").negate())
                    .ifPresent(clusterBuilder::withClusterName);
            java.util.Optional.ofNullable(maxSchemaAgreementWaitSeconds)
                    .filter(value -> value > 0)
                    .ifPresent(clusterBuilder::withMaxSchemaAgreementWaitSeconds);
            java.util.Optional.ofNullable(protocolVersion).map(ProtocolVersion::name)
                    .map(com.datastax.driver.core.ProtocolVersion::valueOf)
                    .ifPresent(clusterBuilder::withProtocolVersion);
            java.util.Optional.ofNullable(compression).map(ProtocolCompression::name)
                    .map(com.datastax.driver.core.ProtocolOptions.Compression::valueOf)
                    .ifPresent(clusterBuilder::withCompression);
            if (sslEnabled) {
                clusterBuilder.withSSL();
            }
            Cluster cluster = clusterBuilder.build();
            logger.info("Connecting to Cassandra Database: {} , port: {} with clusterName: {} , protocol version {} and compression type {} ", host, port, clusterName != null ? clusterName : null, protocolVersion != null ? protocolVersion : null, compression != null ? compression : null);
            Session session = isNotEmpty(keyspace) ? cluster.connect(keyspace) : cluster.connect();
            logger.info("Connected to Cassandra Cluster: {} !", session.getCluster().getClusterName());
            return new CassandraConnection(cluster, session);
        } catch (Exception e) {
            throw new CassandraException(e);
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

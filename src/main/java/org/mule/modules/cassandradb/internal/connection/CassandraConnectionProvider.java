package org.mule.modules.cassandradb.internal.connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ProtocolOptions;
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

import static com.datastax.driver.core.Cluster.builder;
import static com.datastax.driver.core.ProtocolVersion.valueOf;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class CassandraConnectionProvider extends ConnectorConnectionProvider<CassandraConnection> implements CachedConnectionProvider<CassandraConnection> {

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

    private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionProvider.class);

    @Override
    public CassandraConnection connect() throws ConnectionException {
        Builder clusterBuilder;
        validateAttribute(host);
        validateAttribute(port);
        try {
            clusterBuilder = builder().addContactPoint(host).withPort(Integer.parseInt(port));
        } catch (IllegalArgumentException connEx) {
            logger.error("Error while connecting to Cassandra database!", connEx);
            throw new CassandraException(connEx.getMessage());
        }
        clusterBuilder = isNotEmpty(username)&&isNotEmpty(password)? clusterBuilder.withCredentials(username, password): clusterBuilder;
        clusterBuilder = isNotEmpty(clusterName)? clusterBuilder.withClusterName(clusterName): clusterBuilder;
        clusterBuilder = maxSchemaAgreementWaitSeconds > 0 ?  clusterBuilder.withMaxSchemaAgreementWaitSeconds(maxSchemaAgreementWaitSeconds): clusterBuilder;
        clusterBuilder = protocolVersion != null ? clusterBuilder.withProtocolVersion(valueOf(protocolVersion.name())) : clusterBuilder;
        clusterBuilder = compression != null ? clusterBuilder.withCompression(ProtocolOptions.Compression.valueOf(compression.name())) : clusterBuilder;
        clusterBuilder = sslEnabled? clusterBuilder.withSSL(): clusterBuilder;
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

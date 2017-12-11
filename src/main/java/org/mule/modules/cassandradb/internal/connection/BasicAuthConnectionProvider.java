package org.mule.modules.cassandradb.internal.connection;

import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import org.mule.modules.cassandradb.internal.exception.CassandraException;
import org.mule.runtime.api.connection.CachedConnectionProvider;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionValidationResult;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Password;
import org.mule.runtime.extension.api.annotation.param.display.Placement;

import static org.mule.runtime.api.connection.ConnectionValidationResult.failure;
import static org.mule.runtime.api.connection.ConnectionValidationResult.success;

public class BasicAuthConnectionProvider implements CachedConnectionProvider<CassandraConnection> {

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
     *            the username to use for authentication.
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
    private ProtocolOptions.Compression compression;

    /**
     * Enables the use of SSL for the cluster.
     */
    @Parameter
    @Optional
    @Placement(tab = "Advanced Settings")
    @DisplayName("SSL")
    private boolean sslEnabled;

    @Override
    public CassandraConnection connect() throws ConnectionException {
        AdvancedConnectionParameters advancedConnectionParameters = new AdvancedConnectionParameters(protocolVersion, clusterName, maxSchemaAgreementWaitSeconds, compression, sslEnabled);
        return CassandraConnection.build(new ConnectionParameters(host, port, username,
                password, keyspace, advancedConnectionParameters));
    }

    @Override
    public void disconnect(CassandraConnection cassandraConnection) {
        cassandraConnection.disconnect();
    }

    @Override
    public ConnectionValidationResult validate(CassandraConnection cassandraConnection) {
        try {
            cassandraConnection.validate();
            return success();
        } catch (CassandraException e) {
            return failure("Connection is no longer valid", e);
        }
    }
}

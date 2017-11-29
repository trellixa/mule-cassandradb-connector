/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.internal.connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.commons.lang3.StringUtils;

import org.mule.modules.cassandradb.internal.exception.CassandraException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.mule.connectors.commons.template.connection.ConnectorConnection;


public final class CassandraConnection implements ConnectorConnection {
    /**
     * Cassandra Cluster.
     */
    private Cluster cluster;
    /**
     * Cassandra Session.
     */
    private Session cassandraSession;

    private static final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);

    /**
     * Connect to Cassandra Cluster specified by provided host IP
     * address and port number.
     *
     * @param connectionParameters the connection parameters
     * @return CassandraConnection created
     * @throws CassandraException if any error occurs when trying to connect
     */
    public static CassandraConnection buildCassandraClient(ConnectionParameters connectionParameters) throws CassandraException {
        validateBasicParams(connectionParameters);
        Cluster.Builder clusterBuilder;
        try {
            clusterBuilder = Cluster.builder().addContactPoint(connectionParameters.getHost()).withPort(Integer.parseInt(connectionParameters.getPort()));
        } catch (IllegalArgumentException connEx) {
            logger.error("Error while connecting to Cassandra database!", connEx);
            throw new CassandraException(connEx.getMessage());
        }

        if (StringUtils.isNotEmpty(connectionParameters.getUsername()) && StringUtils.isNotEmpty(connectionParameters.getPassword())) {
            clusterBuilder.withCredentials(connectionParameters.getUsername(), connectionParameters.getPassword());
        }

        if (connectionParameters.getAdvancedConnectionParameters() != null) {
            addAdvancedConnectionParameters(clusterBuilder, connectionParameters.getAdvancedConnectionParameters());
        }

        CassandraConnection client = new CassandraConnection();
        client.cluster = clusterBuilder.build();

        try {
            logger.info("Connecting to Cassandra Database: {} , port: {} with clusterName: {} , protocol version {} and compression type {} ",
                    connectionParameters.getHost(),
                    connectionParameters.getPort(),
                    connectionParameters.getAdvancedConnectionParameters() != null ? connectionParameters.getAdvancedConnectionParameters().getClusterName() : null,
                    connectionParameters.getAdvancedConnectionParameters() != null ? connectionParameters.getAdvancedConnectionParameters().getProtocolVersion() : null,
                    connectionParameters.getAdvancedConnectionParameters() != null ? connectionParameters.getAdvancedConnectionParameters().getCompression() : null);
            client.cassandraSession = StringUtils.isNotEmpty(connectionParameters.getKeyspace()) ? client.cluster.connect(connectionParameters.getKeyspace())
                    : client.cluster.connect();
            logger.info("Connected to Cassandra Cluster: {} !", client.cassandraSession.getCluster().getClusterName());
        } catch (Exception cassandraException) {
            logger.error("Error while connecting to Cassandra database!", cassandraException);
            throw new CassandraException(cassandraException.getMessage());
        }
        return client;
    }

    private static void addAdvancedConnectionParameters(Cluster.Builder clusterBuilder, AdvancedConnectionParameters advancedConnectionParameters) {
        if (StringUtils.isNotEmpty(advancedConnectionParameters.getClusterName())) {
            clusterBuilder.withClusterName(advancedConnectionParameters.getClusterName());
        }

        if (advancedConnectionParameters.getMaxSchemaAgreementWaitSeconds() > 0) {
            clusterBuilder.withMaxSchemaAgreementWaitSeconds(advancedConnectionParameters.getMaxSchemaAgreementWaitSeconds());
        }

        if (advancedConnectionParameters.getProtocolVersion() != null) {
            clusterBuilder.withProtocolVersion(advancedConnectionParameters.getProtocolVersion());
        }

        if (advancedConnectionParameters.getCompression() != null) {
            clusterBuilder.withCompression(advancedConnectionParameters.getCompression());
        }

        if (advancedConnectionParameters.isSsl()) {
            clusterBuilder.withSSL();
        }
    }

    private void closeCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private void closeSession() {
        if (cassandraSession != null) {
            cassandraSession.close();
        }
    }

    private static void validateBasicParams(ConnectionParameters parameters) {
        if (StringUtils.isBlank(parameters.getHost()) || StringUtils.isBlank(parameters.getPort())) {
            throw new IllegalArgumentException("Unable to connect! Missing HOST or PORT parameter!");
        }
    }

    @Override
    public void disconnect() {
        closeSession();
        closeCluster();
    }

    @Override
    public void validate() {
        if (cassandraSession == null) {
            throw new CassandraException("Connection is invalid");
        }
    }

    public Session getCassandraSession() {
        return cassandraSession;
    }
}

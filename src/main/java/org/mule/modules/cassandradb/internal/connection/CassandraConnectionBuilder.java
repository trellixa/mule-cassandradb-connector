package org.mule.modules.cassandradb.internal.connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import org.apache.commons.lang3.StringUtils;
import org.mule.modules.cassandradb.internal.exception.CassandraException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// FIXME: This class is not necessary, the connection building process should be part of the connection provier.
public class CassandraConnectionBuilder {

    private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionBuilder.class);

    public static CassandraConnection build(ConnectionParameters connectionParameters)throws CassandraException {
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

        Cluster cluster = clusterBuilder.build();
        Session session = null;

        try {
            logger.info("Connecting to Cassandra Database: {} , port: {} with clusterName: {} , protocol version {} and compression type {} ",
                    connectionParameters.getHost(),
                    connectionParameters.getPort(),
                    connectionParameters.getAdvancedConnectionParameters() != null ? connectionParameters.getAdvancedConnectionParameters().getClusterName() : null,
                    connectionParameters.getAdvancedConnectionParameters() != null ? connectionParameters.getAdvancedConnectionParameters().getProtocolVersion() : null,
                    connectionParameters.getAdvancedConnectionParameters() != null ? connectionParameters.getAdvancedConnectionParameters().getCompression() : null);
            session = StringUtils.isNotEmpty(connectionParameters.getKeyspace()) ? cluster.connect(connectionParameters.getKeyspace())
                    : cluster.connect();
            logger.info("Connected to Cassandra Cluster: {} !", session.getCluster().getClusterName());
        } catch (Exception cassandraException) {
            logger.error("Error while connecting to Cassandra database!", cassandraException);
            throw new CassandraException(cassandraException.getMessage());
        }
        return new CassandraConnection(cluster, session);
    }

    private static void addAdvancedConnectionParameters(Cluster.Builder clusterBuilder, AdvancedConnectionParameters advancedConnectionParameters) {
        if (StringUtils.isNotEmpty(advancedConnectionParameters.getClusterName())) {
            clusterBuilder.withClusterName(advancedConnectionParameters.getClusterName());
        }

        if (advancedConnectionParameters.getMaxSchemaAgreementWaitSeconds() > 0) {
            clusterBuilder.withMaxSchemaAgreementWaitSeconds(advancedConnectionParameters.getMaxSchemaAgreementWaitSeconds());
        }

        if (advancedConnectionParameters.getProtocolVersion() != null) {
            ProtocolVersion cassandraProtocolVersion = ProtocolVersion.valueOf(advancedConnectionParameters.getProtocolVersion().name());
            clusterBuilder.withProtocolVersion(cassandraProtocolVersion);
        }

        if (advancedConnectionParameters.getCompression() != null) {
            ProtocolOptions.Compression compression = ProtocolOptions.Compression.valueOf(advancedConnectionParameters.getCompression().name());
            clusterBuilder.withCompression(compression);
        }

        if (advancedConnectionParameters.isSsl()) {
            clusterBuilder.withSSL();
        }
    }

    private static void validateBasicParams(ConnectionParameters parameters) {
        if (StringUtils.isBlank(parameters.getHost()) || StringUtils.isBlank(parameters.getPort())) {
            throw new IllegalArgumentException("Unable to connect! Missing HOST or PORT parameter!");
        }
    }
}

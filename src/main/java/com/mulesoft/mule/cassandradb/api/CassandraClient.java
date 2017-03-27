package com.mulesoft.mule.cassandradb.api;

import com.datastax.driver.core.*;
import com.mulesoft.mule.cassandradb.metadata.CassQueryVisitor;
import com.mulesoft.mule.cassandradb.utils.builders.HelperStatements;
import org.apache.commons.lang3.StringUtils;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.annotations.Query;
import org.mule.api.annotations.QueryTranslator;
import org.mule.api.annotations.display.Placement;
import org.mule.common.query.DsqlQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class CassandraClient {

    /**
     * Cassandra Cluster.
     */
    private Cluster cluster;
    /**
     * Cassandra Session.
     */
    private Session cassandraSession;

    final static Logger logger = LoggerFactory.getLogger(CassandraClient.class);

    /**
     * Connect to Cassandra Cluster specified by provided node IP
     * address and port number.
     *
     * @param node     Cluster node IP address.
     * @param port     Port of cluster host.
     * @param username the username to buildCassandraClient with
     * @param keyspace optional - keyspace to retrieve cluster session for
     */
    public static CassandraClient buildCassandraClient(final String node, final int port, final String username, final String password, final String keyspace) throws org.mule.api.ConnectionException {
        Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPoint(node)
                .withPort(port);

        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            clusterBuilder.withCredentials(username, password);
        }

        CassandraClient client = new CassandraClient();
        client.cluster = clusterBuilder.build();

        try {
            logger.info(String.format("Connecting to Cassandra Database: %s , port: %s", node, port));
            client.cassandraSession = StringUtils.isNotEmpty(keyspace) ? client.cluster.connect(keyspace) : client.cluster.connect();
            logger.info(String.format("Connected to Cassandra Cluster Node: %s!", client.cassandraSession.getCluster().getClusterName()));
        } catch (Exception cassandraException) {
            logger.error("Error while connecting to Cassandra database!", cassandraException);
            throw new org.mule.api.ConnectionException(ConnectionExceptionCode.UNKNOWN, null, cassandraException.getMessage());
        }
        return client;
    }

    public boolean createKeyspace(String keyspaceName, Map<String, Object> replicationStrategy) {
        return cassandraSession.execute(HelperStatements.createKeyspaceStatement(keyspaceName, replicationStrategy).getQueryString()).wasApplied();
    }

    public boolean dropKeyspace(String keyspaceName) {
        return cassandraSession.execute(HelperStatements.dropKeyspaceStatement(keyspaceName).getQueryString()).wasApplied();
    }

    public boolean createTable(String tableName, String customKeyspaceName, Map<String, Object> partitionKey) {
        return cassandraSession.execute(HelperStatements.createTable(tableName,
                StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : cassandraSession.getLoggedKeyspace(), partitionKey)).wasApplied();
    }

    public boolean dropTable(String tableName, String customKeyspaceName) {
        return cassandraSession.execute(HelperStatements.dropTable(tableName,
                StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : cassandraSession.getLoggedKeyspace())).wasApplied();
    }

    public ResultSet executeCQLQuery(String cqlQuery) {
        if (StringUtils.isNotBlank(cqlQuery)) {
            return cassandraSession.execute(cqlQuery);
        }
        return null;
    }

    public List<String> getTableNamesFromKeyspace(String keyspaceName) {
        if (StringUtils.isNotBlank((keyspaceName))) {
            logger.info("Retrieving table names from the keyspace: {0} ...", keyspaceName);
            Collection<TableMetadata> tables = cluster
                    .getMetadata().getKeyspace(keyspaceName)
                    .getTables();
            ArrayList<String> tableNames = new ArrayList<String>();
            for (TableMetadata table : tables) {
                tableNames.add(table.getName());
            }
            return tableNames;
        }
        return null;
    }

    /**
     * Fetches table metadata using DataStax java driver, based on the keyspace provided
     *
     * @return the table metadata as returned by the driver.
     */
    public TableMetadata fetchTableMetadata(final String keyspaceUsed, final String tableName) {
        if (StringUtils.isNotBlank(tableName)) {
            logger.info("Retrieving table metadata for: {0} ...", tableName);
            Metadata metadata = cluster.getMetadata();
            KeyspaceMetadata ksMetadata = metadata.getKeyspace(keyspaceUsed);
            if (ksMetadata != null) {
                return ksMetadata.getTable(tableName);
            }
        }
        return null;
    }

    public String getLoggedKeyspace() {
        return cassandraSession.getLoggedKeyspace();
    }


    /**
     * Close cluster.
     */
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

    public void close() {
        closeSession();
        closeCluster();
    }


    /**
     * Provide my Session.
     *
     * @return My session.
     */
//    public Session getSession() {
//        return this.cassandraSession;
//    }

//    /**
//     * Provide cluster used.
//     */
//    public Cluster getCluster() {
//        return this.cluster;
//    }

}


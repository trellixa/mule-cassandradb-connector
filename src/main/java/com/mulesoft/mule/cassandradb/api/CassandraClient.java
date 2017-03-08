package com.mulesoft.mule.cassandradb.api;

import com.datastax.driver.core.*;
import org.apache.commons.httpclient.auth.CredentialsProvider;
import org.apache.commons.lang3.StringUtils;
import org.mule.api.ConnectionExceptionCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraClient {

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
     * @param username the username to connect with
     * @param keyspace optional - keyspace to retrieve cluster session for
     */
    public void connect(final String node, final int port, final String username, final String password, final String keyspace) throws org.mule.api.ConnectionException {
        Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPoint(node)
                .withPort(port);

        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            clusterBuilder.withCredentials(username, password);
        }

        cluster = clusterBuilder.build();

        try {
            logger.info(String.format("Connecting to Cassandra Database: %s , port: %s", node, port));
            cassandraSession = StringUtils.isNotEmpty(keyspace) ? cluster.connect(keyspace) : cluster.connect();
            logger.info(String.format("Connected to Cassandra Cluster Node: %s!", cassandraSession.getCluster().getClusterName()));
        } catch (Exception cassandraException) {
            logger.error("Error while connecting to Cassandra database!", cassandraException);
            throw new org.mule.api.ConnectionException(ConnectionExceptionCode.UNKNOWN, null, cassandraException.getMessage());
        } finally {
            close();
        }
    }

    /** Close cluster. */
    private void closeCluster()
    {
        cluster.close();
    }

    private void closeSession() {
        if (cassandraSession != null) {
            cassandraSession.close();
        }
    }

    public void close() {
        closeCluster();
        closeSession();
    }


    /**
     * Provide my Session.
     *
     * @return My session.
     */
    public Session getSession() {
        return this.cassandraSession;
    }

    /**
     * Provide cluster used.
     */
    public Cluster getCluster() {
        return this.cluster;
    }

}


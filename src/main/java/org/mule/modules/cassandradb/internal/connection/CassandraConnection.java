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

    public CassandraConnection(Cluster cluster, Session session) {
        this.cluster = cluster;
        this.cassandraSession = session;
    }

    /**
     * Connect to Cassandra Cluster specified by provided host IP
     * address and port number.
     *
     * @param connectionParameters the connection parameters
     * @return CassandraConnection created
     * @throws CassandraException if any error occurs when trying to connect
     */
    public static CassandraConnection build(ConnectionParameters connectionParameters) throws CassandraException {
        return CassandraConnectionBuilder.build(connectionParameters);
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

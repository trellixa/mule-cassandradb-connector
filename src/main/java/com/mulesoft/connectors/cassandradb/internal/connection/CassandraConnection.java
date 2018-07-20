/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.connectors.cassandradb.internal.connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.mulesoft.connectors.cassandradb.internal.exception.CassandraException;
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

    public CassandraConnection(Cluster cluster, Session session) {
        this.cluster = cluster;
        this.cassandraSession = session;
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

    public Cluster getCluster() {
        return cluster;
    }
}
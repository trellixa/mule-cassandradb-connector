package com.mulesoft.connectors.cassandradb.internal.metadata;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.mulesoft.connectors.cassandradb.internal.connection.CassandraConnection;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class CassandraMetadata {

    private CassandraConnection cassandraConnection;

    public CassandraMetadata(CassandraConnection connection) {
        this.cassandraConnection = connection;
    }

    public KeyspaceMetadata getKeyspaceMetadata(String keyspaceName) {
        return cassandraConnection.getCluster().getMetadata().getKeyspace(keyspaceName);
    }

    public TableMetadata getTableMetadata(String keyspaceName, String tableName) {
        if (isNotBlank(tableName)) {
            KeyspaceMetadata ksMetadata = getKeyspaceMetadata(keyspaceName);
            if (ksMetadata != null) {
                return ksMetadata.getTable(tableName);
            }
        }
        return null;
    }
}

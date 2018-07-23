/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
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

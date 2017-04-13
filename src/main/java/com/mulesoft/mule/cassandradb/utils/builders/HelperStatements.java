/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.utils.builders;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.*;
import com.mulesoft.mule.cassandradb.utils.Constants;
import com.mulesoft.mule.cassandradb.utils.ReplicationStrategy;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.Map;

public class HelperStatements {

    public static SchemaStatement createKeyspaceStatement(String keyspaceName, Map<String, Object> replicationStrategy) {
        //build create keyspace statement if not exists
        CreateKeyspace createKeyspaceStatement = SchemaBuilder.createKeyspace(keyspaceName).ifNotExists();

        return createKeyspaceStatement.with().replication(ReplicationStrategy.buildReplicationStrategy(replicationStrategy));
    }

    public static SchemaStatement dropKeyspaceStatement(String keyspaceName) {
        return SchemaBuilder.dropKeyspace(keyspaceName).ifExists();
    }

    public static SchemaStatement createTable(String tableName, String keyspaceName, Map<String, Object> partitionKey) {
        //build drop keyspace statement
        ImmutablePair<String, DataType> partitionKeyInfo = resolvePartitionKey(partitionKey);

        return SchemaBuilder.createTable(keyspaceName, tableName).ifNotExists().
                addPartitionKey(partitionKeyInfo.getLeft(), partitionKeyInfo.getRight());
    }

    public static SchemaStatement addColumnToTable(String tableName, String keyspaceName, String columnName, DataType columnType) {
        return SchemaBuilder.alterTable(keyspaceName, tableName).addColumn(columnName).type(columnType);
    }

    public static SchemaStatement dropTable(String tableName, String keyspaceName) {
        return SchemaBuilder.dropTable(keyspaceName, tableName).ifExists();
    }

    /**
     * extract partition key column information or return default values
     * @param partitionKey
     * @return
     */
    private static ImmutablePair<String, DataType> resolvePartitionKey(Map<String, Object> partitionKey) {
        MutablePair<String, DataType> partitionKeyInfo = new MutablePair<String, DataType>();

        if (partitionKey == null) {
            partitionKeyInfo.setLeft(Constants.DUMMY_PARTITION_KEY);
            partitionKeyInfo.setRight(DataType.text());
        } else {
            partitionKeyInfo.setLeft(String.valueOf(partitionKey.get(Constants.PARTITION_KEY_COLUMN_NAME)));
            partitionKeyInfo.setRight((DataType) partitionKey.get(Constants.DATA_TYPE));
        }

        return new ImmutablePair<String, DataType>(partitionKeyInfo.getLeft(), partitionKeyInfo.getRight());
    }
}

package com.mulesoft.mule.cassandradb.utils.builders;

import com.datastax.driver.core.schemabuilder.CreateKeyspace;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.mulesoft.mule.cassandradb.utils.ReplicationStrategy;

import java.util.Map;

public class HelperStatements {

    public static KeyspaceOptions createKeyspaceStatement(String keyspaceName, Map<String, Object> replicationStrategy) {
        //build create keyspace statement if not exists
        CreateKeyspace createKeyspace = SchemaBuilder.createKeyspace(keyspaceName).ifNotExists();
        //execute statement
        return createKeyspace.with()
                .replication(ReplicationStrategy.buildReplicationStrategy(replicationStrategy));
    }
}

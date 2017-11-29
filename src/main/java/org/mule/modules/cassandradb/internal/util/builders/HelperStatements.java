/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.internal.util.builders;

import com.datastax.driver.core.schemabuilder.CreateKeyspace;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import org.mule.modules.cassandradb.internal.operation.parameters.CreateKeyspaceInput;
import org.mule.modules.cassandradb.internal.util.ReplicationStrategy;


public class HelperStatements {
	
	private HelperStatements() {
		// Empty constructor
	}

    public static SchemaStatement createKeyspaceStatement(CreateKeyspaceInput input) {
        //build create keyspace statement if not exists
        CreateKeyspace createKeyspaceStatement = SchemaBuilder.createKeyspace(input.getKeyspaceName()).ifNotExists();

        return createKeyspaceStatement.with().replication(ReplicationStrategy.buildReplicationStrategy(input));
    }
}

/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.DataCenter;
import org.mule.modules.cassandradb.internal.exception.CassandraError;

import static org.junit.Assert.assertTrue;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.SIMPLE;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.*;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.NETWORK_TOPOLOGY;

public class CreateKeyspaceTestCases extends AbstractTestCases {

    @Test
    public void testCreateKeyspaceWithDefaultReplicationStrategyWithSuccess() throws Exception {
        CreateKeyspaceInput createKeyspaceInput = new CreateKeyspaceInput();
        createKeyspaceInput.setKeyspaceName(KEYSPACE_NAME_1);
        assertTrue(createKeyspace(createKeyspaceInput));
    }

    @Test
    public void testCreateKeyspaceWithDifferentReplicationStrategyWithSuccess() throws Exception {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(KEYSPACE_NAME_2);
        keyspaceInput.setFirstDataCenter(new DataCenter(DATA_CENTER_NAME, 1));
        keyspaceInput.setReplicationStrategyClass(NETWORK_TOPOLOGY.getStrategyClass());

        assertTrue(createKeyspace(keyspaceInput));
    }

    @Test
    public void testCreateKeyspaceWithInvalidReplicationStrategy() throws Exception {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(KEYSPACE_NAME_3);
        keyspaceInput.setReplicationFactor(3);
        keyspaceInput.setReplicationStrategyClass("SomeReplicationStrategy");

        createKeyspaceExpException(keyspaceInput, CassandraError.UNKNOWN);
    }

    @Test
    public void shouldFail_Using_MissingReplicationFactor() throws Exception {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(KEYSPACE_NAME_1);
        keyspaceInput.setReplicationFactor(null);
        keyspaceInput.setReplicationStrategyClass(SIMPLE.getStrategyClass());

        createKeyspaceExpException(keyspaceInput, CassandraError.UNKNOWN);
    }
}

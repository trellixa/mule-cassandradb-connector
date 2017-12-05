/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.KeyspaceMetadata;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.DataCenter;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.SIMPLE;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.*;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.NETWORK_TOPOLOGY;

public class CreateKeyspaceTestCase extends AbstractTestCases {

    private static final int SLEEP_DURATION = 2000;

    @Before
    @After
    public void dropKeyspaces() {
        getCassandraService().dropKeyspace(KEYSPACE_NAME_1);
        getCassandraService().dropKeyspace(KEYSPACE_NAME_2);
        getCassandraService().dropKeyspace(KEYSPACE_NAME_3);
    }

    @Test
    public void testCreateKeyspaceWithDefaultReplicationStrategyWithSuccess() throws Exception {
        CreateKeyspaceInput createKeyspaceInput = new CreateKeyspaceInput();
        createKeyspaceInput.setKeyspaceName(KEYSPACE_NAME_1);
        assertTrue(createKeyspace(createKeyspaceInput));

        Thread.sleep(SLEEP_DURATION);
        verifyResponse(createKeyspaceInput);
    }

    @Test
    public void testCreateKeyspaceWithDifferentReplicationStrategyWithSuccess() throws Exception {
        CreateKeyspaceInput createKeyspaceInput = new CreateKeyspaceInput();
        createKeyspaceInput.setKeyspaceName(KEYSPACE_NAME_2);
        createKeyspaceInput.setFirstDataCenter(new DataCenter(DATA_CENTER_NAME, 1));
        createKeyspaceInput.setReplicationStrategyClass(NETWORK_TOPOLOGY.getStrategyClass());

        assertTrue(createKeyspace(createKeyspaceInput));

        Thread.sleep(SLEEP_DURATION);
        verifyResponse(createKeyspaceInput);
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

    private void verifyResponse(CreateKeyspaceInput keyspaceInput) {
        boolean wasCreated = false;
        KeyspaceMetadata ksMedata = getKeyspaceMetadata(keyspaceInput.getKeyspaceName());
        assertNotNull(ksMedata);

        String replicationStrategyClass = keyspaceInput.getReplicationStrategyClass();
        Map<String, String> replication = ksMedata.getReplication();
        if (StringUtils.isNotBlank(replicationStrategyClass)) {
            assertTrue(StringUtils.endsWithIgnoreCase(replication.get("class"), replicationStrategyClass));
        }
        if (keyspaceInput.getReplicationFactor() != null) {
            assertEquals(keyspaceInput.getReplicationFactor(), replication.get("replication_factor"));
        }
        if (keyspaceInput.getFirstDataCenter() != null) {
            assertTrue(replication.containsKey(keyspaceInput.getFirstDataCenter().getName()));
            String datacenterName = replication.get(keyspaceInput.getFirstDataCenter().getName());
            assertEquals(String.valueOf(keyspaceInput.getFirstDataCenter().getValue()), datacenterName);
        }
    }
}

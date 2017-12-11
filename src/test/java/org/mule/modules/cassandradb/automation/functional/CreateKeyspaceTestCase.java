/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.KeyspaceMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.DataCenter;
import org.mule.modules.cassandradb.automation.util.TestDataBuilder;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.runtime.extension.api.error.ErrorTypeDefinition;

import java.util.Map;

import static org.apache.commons.lang3.StringUtils.endsWithIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.NETWORK_TOPOLOGY;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.SIMPLE;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.SYNTAX_ERROR;
import static org.mule.tck.junit4.matcher.ErrorTypeMatcher.errorType;

public class CreateKeyspaceTestCase extends AbstractTestCases {

    @Before
    @After
    public void dropKeyspaces() {
        getCassandraService().dropKeyspace(TestDataBuilder.KEYSPACE_NAME_1);
        getCassandraService().dropKeyspace(TestDataBuilder.KEYSPACE_NAME_2);
        getCassandraService().dropKeyspace(TestDataBuilder.KEYSPACE_NAME_3);
    }

    @Test
    public void testCreateKeyspaceWithDefaultReplicationStrategyWithSuccess() throws Exception {
        CreateKeyspaceInput createKeyspaceInput = new CreateKeyspaceInput();
        createKeyspaceInput.setKeyspaceName(TestDataBuilder.KEYSPACE_NAME_1);
        assertTrue(createKeyspace(createKeyspaceInput));

        Thread.sleep(SLEEP_DURATION);
        verifyResponse(createKeyspaceInput);
    }

    @Test
    public void testCreateKeyspaceWithDifferentReplicationStrategyWithSuccess() throws Exception {
        CreateKeyspaceInput createKeyspaceInput = new CreateKeyspaceInput();
        createKeyspaceInput.setKeyspaceName(TestDataBuilder.KEYSPACE_NAME_2);
        createKeyspaceInput.setFirstDataCenter(new DataCenter(TestDataBuilder.DATA_CENTER_NAME, 1));
        createKeyspaceInput.setReplicationStrategyClass(NETWORK_TOPOLOGY.getStrategyClass());

        assertTrue(createKeyspace(createKeyspaceInput));

        Thread.sleep(SLEEP_DURATION);
        verifyResponse(createKeyspaceInput);
    }

    @Test
    public void testCreateKeyspaceWithInvalidReplicationStrategy() throws Exception {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(TestDataBuilder.KEYSPACE_NAME_3);
        keyspaceInput.setReplicationFactor(3);
        keyspaceInput.setReplicationStrategyClass("SomeReplicationStrategy");

        createKeyspaceExpException(keyspaceInput, CassandraError.UNKNOWN);
    }

    @Test
    public void shouldFail_Using_MissingReplicationFactor() throws Exception {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(TestDataBuilder.KEYSPACE_NAME_1);
        keyspaceInput.setReplicationFactor(null);
        keyspaceInput.setReplicationStrategyClass(SIMPLE.getStrategyClass());

        createKeyspaceExpException(keyspaceInput, SYNTAX_ERROR);
    }

    private void verifyResponse(CreateKeyspaceInput keyspaceInput) {
        KeyspaceMetadata ksMedata = getKeyspaceMetadata(keyspaceInput.getKeyspaceName());
        assertNotNull(ksMedata);

        String replicationStrategyClass = keyspaceInput.getReplicationStrategyClass();
        Map<String, String> replication = ksMedata.getReplication();
        if (isNotBlank(replicationStrategyClass)) {
            assertTrue(endsWithIgnoreCase(replication.get("class"), replicationStrategyClass));
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

    boolean createKeyspace(final CreateKeyspaceInput keyspaceInput) throws Exception {
        return (boolean) flowRunner("createKeyspace-flow")
                .withPayload(keyspaceInput)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    void createKeyspaceExpException(final CreateKeyspaceInput keyspaceInput, ErrorTypeDefinition errorType) throws Exception {
        flowRunner("createKeyspace-flow")
                .withPayload(keyspaceInput)
                .runExpectingException(errorType(errorType));
    }
}

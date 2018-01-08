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
import org.mule.modules.cassandradb.api.ReplicationStrategy;
import org.mule.modules.cassandradb.automation.util.TestDataBuilder;
import org.mule.runtime.extension.api.error.ErrorTypeDefinition;

import java.util.Map;

import static org.apache.commons.lang3.StringUtils.endsWithIgnoreCase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.NetworkTopologyStrategy;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.SimpleStrategy;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.DATA_CENTER_NAME;
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
        createKeyspace(createKeyspaceInput);

        Thread.sleep(SLEEP_DURATION);
        verifyResponse(createKeyspaceInput);
    }

    @Test
    public void testCreateKeyspaceWithDifferentReplicationStrategyWithSuccess() throws Exception {
        CreateKeyspaceInput createKeyspaceInput = new CreateKeyspaceInput();
        createKeyspaceInput.setKeyspaceName(TestDataBuilder.KEYSPACE_NAME_2);
        DataCenter firstDataCenter = new DataCenter();
        firstDataCenter.setName(DATA_CENTER_NAME);
        firstDataCenter.setValue(1);
        createKeyspaceInput.setFirstDataCenter(firstDataCenter);
        createKeyspaceInput.setReplicationStrategyClass(NetworkTopologyStrategy);

        createKeyspace(createKeyspaceInput);

        Thread.sleep(SLEEP_DURATION);
        verifyResponse(createKeyspaceInput);
    }

    @Test
    public void shouldFail_Using_MissingReplicationFactor() throws Exception {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(TestDataBuilder.KEYSPACE_NAME_1);
        keyspaceInput.setReplicationFactor(null);
        keyspaceInput.setReplicationStrategyClass(SimpleStrategy);

        createKeyspaceExpException(keyspaceInput, SYNTAX_ERROR);
    }

    private void verifyResponse(CreateKeyspaceInput keyspaceInput) {
        KeyspaceMetadata ksMedata = getKeyspaceMetadata(keyspaceInput.getKeyspaceName());
        assertNotNull(ksMedata);

        ReplicationStrategy replicationStrategyClass = keyspaceInput.getReplicationStrategyClass();
        Map<String, String> replication = ksMedata.getReplication();
        if (replicationStrategyClass != null) {
            assertTrue(endsWithIgnoreCase(replication.get("class"), replicationStrategyClass.name()));
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

    void createKeyspace(final CreateKeyspaceInput keyspaceInput) throws Exception {
        flowRunner("createKeyspace-flow" )
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

/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional.processors;

import com.datastax.driver.core.KeyspaceMetadata;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.metadata.CreateKeyspaceInput;
import org.mule.modules.cassandradb.metadata.DataCenter;
import org.mule.modules.cassandradb.utils.CassandraDBException;
import org.mule.modules.cassandradb.utils.ReplicationStrategy;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.List;

public class CreateKeyspaceTestCases extends CassandraAbstractTestCases {

    @AfterClass
    public static void tearDown() {
        cassClient.dropKeyspace(TestsConstants.KEYSPACE_NAME_1);
        cassClient.dropKeyspace(TestsConstants.KEYSPACE_NAME_2);
        cassClient.dropKeyspace(TestsConstants.KEYSPACE_NAME_3);
    }

    @Test
    public void testCreateKeyspaceWithDefaultReplicationStrategyWithSuccess() throws CassandraDBException, InterruptedException {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(TestsConstants.KEYSPACE_NAME_1);

        Assert.assertTrue(getConnector().createKeyspace(keyspaceInput));

        Thread.sleep(2000);
        verifyResponse(keyspaceInput);
    }

    @Test
    public void testCreateKeyspaceWithDifferentReplicationStrategyWithSuccess() throws CassandraDBException, InterruptedException {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(TestsConstants.KEYSPACE_NAME_2);
        keyspaceInput.setFirstDataCenter(new DataCenter(TestsConstants.DATA_CENTER_NAME, 1));
        keyspaceInput.setReplicationStrategyClass(ReplicationStrategy.NETWORK_TOPOLOGY.getStrategyClass());

        Assert.assertTrue(getConnector().createKeyspace(keyspaceInput));

        Thread.sleep(2000);
        verifyResponse(keyspaceInput);
    }

    @Test(expected = CassandraDBException.class)
    public void testCreateKeyspaceWithInvalidReplicationStrategy() throws CassandraDBException {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(TestsConstants.KEYSPACE_NAME_3);
        keyspaceInput.setReplicationFactor(3);
        keyspaceInput.setReplicationStrategyClass("SomeReplicationStrategy");

        getConnector().createKeyspace(keyspaceInput);
    }

    @Test(expected = CassandraDBException.class)
    public void shouldFail_Using_MissingReplicationFactor() throws CassandraDBException {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(TestsConstants.KEYSPACE_NAME_1);
        keyspaceInput.setReplicationFactor(null);
        keyspaceInput.setReplicationStrategyClass(ReplicationStrategy.SIMPLE.getStrategyClass());

        getConnector().createKeyspace(keyspaceInput);
    }

    private void verifyResponse(CreateKeyspaceInput keyspaceInput) {
        boolean wasCreated = false;
        List<KeyspaceMetadata> keyspaces = cassClient.getKeyspaces();
        for(KeyspaceMetadata ksMedata : keyspaces) {
            if (ksMedata.getName().equalsIgnoreCase(keyspaceInput.getKeyspaceName())) {
                wasCreated = true;
                if (StringUtils.isNotBlank(keyspaceInput.getReplicationStrategyClass())) {
                    Assert.assertTrue(StringUtils.endsWithIgnoreCase(ksMedata.getReplication().get("class"), keyspaceInput.getReplicationStrategyClass()));
                }
                if (keyspaceInput.getReplicationFactor() != null) {
                    Assert.assertEquals(keyspaceInput.getReplicationFactor(), ksMedata.getReplication().get("replication_factor"));
                }
                if (keyspaceInput.getFirstDataCenter() != null) {
                    Assert.assertTrue(ksMedata.getReplication().containsKey(keyspaceInput.getFirstDataCenter().getName()));
                    Assert.assertEquals(String.valueOf(keyspaceInput.getFirstDataCenter().getValue()), ksMedata.getReplication().get(keyspaceInput.getFirstDataCenter().getName()));
                }
            }
        }
        Assert.assertTrue(wasCreated);
    }
}

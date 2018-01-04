/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.DataCenter;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.NetworkTopologyStrategy;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.DATA_CENTER_NAME;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.KEYSPACE_NAME_1;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.KEYSPACE_NAME_2;

public class DropKeyspaceTestCase extends AbstractTestCases {

    @Test
    public void testDropSimpleKeyspace() throws Exception {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        String keyspaceName = KEYSPACE_NAME_1;
        keyspaceInput.setKeyspaceName(keyspaceName);
        getCassandraService().createKeyspace(keyspaceInput);

        assertNotNull(getKeyspaceMetadata(keyspaceName));

        assertTrue(dropKeyspace(keyspaceName));

        assertNull(getKeyspaceMetadata(keyspaceName));
    }

    @Test
    public void testDropKeyspaceWithDifferentConfigurations() throws Exception {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        String keyspaceName = KEYSPACE_NAME_2;
        keyspaceInput.setKeyspaceName(keyspaceName);
        DataCenter firstDataCenter = new DataCenter();
        firstDataCenter.setName(DATA_CENTER_NAME);
        firstDataCenter.setValue(1);
        keyspaceInput.setFirstDataCenter(firstDataCenter);
        keyspaceInput.setReplicationStrategyClass(NetworkTopologyStrategy);
        getCassandraService().createKeyspace(keyspaceInput);

        assertNotNull(getKeyspaceMetadata(keyspaceName));

        assertTrue(dropKeyspace(keyspaceName));

        assertNull(getKeyspaceMetadata(keyspaceName));
    }

    boolean dropKeyspace(String keyspaceName) throws Exception {
        return (boolean) flowRunner("deleteKeyspace-flow")
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }
}

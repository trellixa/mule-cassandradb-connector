
/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.DataCenter;

import static org.junit.Assert.fail;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.NetworkTopologyStrategy;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.DATA_CENTER_NAME;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.KEYSPACE_NAME_1;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.KEYSPACE_NAME_2;

public class DropKeyspaceTestCase extends AbstractTestCases {

    @Test
    public void testDropSimpleKeyspace() throws Exception {
        try{
            CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
            keyspaceInput.setKeyspaceName(KEYSPACE_NAME_1);
            createKeyspace(keyspaceInput);
        } catch (Exception e){
            fail();
        }
    }

    @Test
    public void testDropKeyspaceWithDifferentConfigurations() throws Exception {
        try{
            CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
            keyspaceInput.setKeyspaceName(KEYSPACE_NAME_2);
            DataCenter firstDataCenter = new DataCenter();
            firstDataCenter.setName(DATA_CENTER_NAME);
            firstDataCenter.setValue(1);
            keyspaceInput.setFirstDataCenter(firstDataCenter);
            keyspaceInput.setReplicationStrategyClass(NetworkTopologyStrategy);
            createKeyspace(keyspaceInput);
            dropKeyspace(KEYSPACE_NAME_2);
        } catch (Exception e){
            fail();
        }
    }
}

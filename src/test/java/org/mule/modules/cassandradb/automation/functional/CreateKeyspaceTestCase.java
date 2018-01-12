/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.DataCenter;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.NetworkTopologyStrategy;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.SimpleStrategy;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.DATA_CENTER_NAME;

public class CreateKeyspaceTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        cleanKeyspaces();
    }

    @After
    public void tearDown() throws Exception {
        cleanKeyspaces();
    }

    @Test
    public void testCreateKeyspaceWithDefaultReplicationStrategyWithSuccess() throws Exception {
        try{
            CreateKeyspaceInput createKeyspaceInput = new CreateKeyspaceInput();
            createKeyspaceInput.setKeyspaceName(TestDataBuilder.KEYSPACE_NAME_1);
            createKeyspace(createKeyspaceInput);
        } catch (Exception e){
            fail();
        }
    }

    @Test
    public void testCreateKeyspaceWithDifferentReplicationStrategyWithSuccess() {
        try{
            CreateKeyspaceInput createKeyspaceInput = new CreateKeyspaceInput();
            createKeyspaceInput.setKeyspaceName(TestDataBuilder.KEYSPACE_NAME_2);
            DataCenter firstDataCenter = new DataCenter();
            firstDataCenter.setName(DATA_CENTER_NAME);
            firstDataCenter.setValue(1);
            createKeyspaceInput.setFirstDataCenter(firstDataCenter);
            createKeyspaceInput.setReplicationStrategyClass(NetworkTopologyStrategy);
            createKeyspace(createKeyspaceInput);
        } catch (Exception e){
            fail();
        }
    }

    @Test
    public void shouldFail_Using_MissingReplicationFactor() throws Exception {
        try{
            CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
            keyspaceInput.setKeyspaceName(TestDataBuilder.KEYSPACE_NAME_1);
            keyspaceInput.setReplicationFactor(null);
            keyspaceInput.setReplicationStrategyClass(SimpleStrategy);
            createKeyspace(keyspaceInput);
        } catch (Exception e){
            assertThat(e.getMessage(), is("Invalid property value: NULL for property: 'replication_factor'."));
        }
    }

    private void cleanKeyspaces() throws Exception {
        dropKeyspace(TestDataBuilder.KEYSPACE_NAME_1);
        dropKeyspace(TestDataBuilder.KEYSPACE_NAME_2);
        dropKeyspace(TestDataBuilder.KEYSPACE_NAME_3);
    }
}

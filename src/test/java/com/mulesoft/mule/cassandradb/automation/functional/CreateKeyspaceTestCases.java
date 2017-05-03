/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional;

import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.metadata.CreateKeyspaceInput;
import com.mulesoft.mule.cassandradb.metadata.DataCenter;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateKeyspaceTestCases extends CassandraDBConnectorAbstractTestCase {

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;
    private final static String keyspaceName1 = "keyspaceName1";
    private final static String keyspaceName2 = "keyspaceName2";
    private final static String keyspaceName3 = "keyspaceName3";

    @BeforeClass
    public static void setup() throws Exception {
        cassConfig = getClientConfig();
        //get instance of cass client based on the configs
        cassClient = CassandraClient.buildCassandraClient(cassConfig.getHost(), cassConfig.getPort(), null, null, null);
        assert cassClient != null;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        cassClient.dropKeyspace(keyspaceName1);
        cassClient.dropKeyspace(keyspaceName2);
    }

    @Test
    public void testCreateKeyspaceWithDefaultReplicationStrategyWithSuccess() throws CassandraDBException {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(keyspaceName1);

        getConnector().createKeyspace(keyspaceInput);
    }

    @Test
    public void testCreateKeyspaceWithDifferentReplicationStrategyWithSuccess() throws CassandraDBException {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(keyspaceName2);
        keyspaceInput.setFirstDataCenter(new DataCenter("datacenter1",1));
        keyspaceInput.setReplicationStrategyClass("NetworkTopologyStrategy");

        getConnector().createKeyspace(keyspaceInput);
    }

    @Test(expected = CassandraDBException.class)
    public void testCreateKeyspaceWithInvalidReplicationStrategy() throws CassandraDBException {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(keyspaceName3);
        keyspaceInput.setReplicationFactor(3);
        keyspaceInput.setReplicationStrategyClass("SomeReplicationStrategy");

        getConnector().createKeyspace(keyspaceInput);
    }
}

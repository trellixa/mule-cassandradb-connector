package org.mule.modules.cassandradb.automation.functional.processors;

import org.junit.Test;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.metadata.CreateKeyspaceInput;
import org.mule.modules.cassandradb.metadata.DataCenter;
import org.mule.modules.cassandradb.utils.CassandraDBException;
import org.mule.modules.cassandradb.utils.ReplicationStrategy;

import static org.junit.Assert.assertTrue;

public class DropKeyspaceTestCases extends CassandraAbstractTestCases {

    @Test
    public void testDropSimpleKeyspace() throws CassandraDBException {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(TestsConstants.KEYSPACE_NAME_1);

        getConnector().createKeyspace(keyspaceInput);

        boolean dropResult = getConnector().dropKeyspace(TestsConstants.KEYSPACE_NAME_1);

        assertTrue(dropResult);
    }

    @Test
    public void testDropKeyspaceWithDifferentConfigurations() throws CassandraDBException {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(TestsConstants.KEYSPACE_NAME_2);
        keyspaceInput.setFirstDataCenter(new DataCenter(TestsConstants.DATA_CENTER_NAME, 1));
        keyspaceInput.setReplicationStrategyClass(ReplicationStrategy.NETWORK_TOPOLOGY.name());

        getConnector().createKeyspace(keyspaceInput);

        boolean dropResult = getConnector().dropKeyspace(TestsConstants.KEYSPACE_NAME_2);

        assertTrue(dropResult);
    }
}

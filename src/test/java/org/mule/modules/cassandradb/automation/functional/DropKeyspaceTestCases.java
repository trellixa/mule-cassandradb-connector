/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.DataCenter;
import org.mule.modules.cassandradb.automation.util.TestsConstants;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mule.modules.cassandradb.api.ReplicationStrategy.NETWORK_TOPOLOGY;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.KEYSPACE_NAME_1;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.KEYSPACE_NAME_2;

public class DropKeyspaceTestCases extends AbstractTestCases {

    @Test
    public void testDropSimpleKeyspace() throws Exception {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(KEYSPACE_NAME_1);

        getCassandraService().createKeyspace(keyspaceInput);

        assertTrue(dropKeyspace(KEYSPACE_NAME_1));
    }

    @Test
    public void testDropKeyspaceWithDifferentConfigurations() throws Exception {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(KEYSPACE_NAME_2);
        keyspaceInput.setFirstDataCenter(new DataCenter(TestsConstants.DATA_CENTER_NAME, 1));
        keyspaceInput.setReplicationStrategyClass(NETWORK_TOPOLOGY.getStrategyClass());

        getCassandraService().createKeyspace(keyspaceInput);

        assertTrue(dropKeyspace(KEYSPACE_NAME_2));
    }
}

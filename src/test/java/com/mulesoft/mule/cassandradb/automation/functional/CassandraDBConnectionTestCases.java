/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional;

import org.junit.Test;

import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.configurations.AdvancedConnectionParameters;
import com.mulesoft.mule.cassandradb.configurations.ConnectionParameters;
import com.mulesoft.mule.cassandradb.util.ConstantsTest;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;

public class CassandraDBConnectionTestCases extends CassandraDBConnectorAbstractTestCase {

    private static CassandraConfig cassConfig;

    @Test
    public void testCassandraBasicConnection() throws Exception {
        cassConfig = getClientConfig();
        CassandraClient cassClient = CassandraClient.buildCassandraClient(new ConnectionParameters(cassConfig.getHost(), cassConfig.getPort(), null, null, null, null));
        assert cassClient != null;
    }

    @Test
    public void testCassandraAdvancedConnection() throws Exception {
        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V2, ConstantsTest.CLUSTER_NAME, ConstantsTest.MAX_WAIT, ProtocolOptions.Compression.NONE, false);
        cassConfig = getClientConfig();
        CassandraClient cassClient = CassandraClient.buildCassandraClient(new ConnectionParameters(cassConfig.getHost(), cassConfig.getPort(), null, null, null, advancedParams));
        assert cassClient != null;
    }

}

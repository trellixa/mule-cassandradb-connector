package com.mulesoft.mule.cassandradb.automation.functional;

import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.configurations.AdvancedConnectionParameters;
import com.mulesoft.mule.cassandradb.configurations.ConnectionParameters;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import org.junit.Test;

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
        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V2, "newClasterName", 10, ProtocolOptions.Compression.NONE, false);
        cassConfig = getClientConfig();
        CassandraClient cassClient = CassandraClient.buildCassandraClient(new ConnectionParameters(cassConfig.getHost(), cassConfig.getPort(), null, null, null, advancedParams));
        assert cassClient != null;
    }

}

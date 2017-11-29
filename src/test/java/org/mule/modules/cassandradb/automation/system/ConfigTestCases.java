/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.system;

import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import org.junit.Test;
import org.mule.modules.cassandradb.automation.functional.AbstractTestCases;
import org.mule.modules.cassandradb.internal.exception.CassandraException;
import org.mule.modules.cassandradb.internal.connection.AdvancedConnectionParameters;
import org.mule.modules.cassandradb.internal.connection.CassandraConnection;
import org.mule.modules.cassandradb.internal.connection.ConnectionParameters;
import org.mule.modules.cassandradb.automation.util.CassandraProperties;
import org.mule.modules.cassandradb.automation.util.TestsConstants;


public class ConfigTestCases {

    private static String INVALID_HOST = "INVALID_HOST";

    private static CassandraProperties cassProperties = AbstractTestCases.getCassandraProperties();

    @Test
    public void shouldConnect_Using_BasicParams() throws Exception {
        ConnectionParameters connectionParameters = new ConnectionParameters(cassProperties.getHost(), cassProperties.getPort(), null, null, null, null);
        CassandraConnection cassClient = CassandraConnection.build(connectionParameters);
        assert cassClient != null;
    }

    @Test
    public void shouldConnect_Using_AdvancedParams() throws Exception {
        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V3, TestsConstants.CLUSTER_NAME, TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);
        cassProperties = AbstractTestCases.getCassandraProperties();
        ConnectionParameters connectionParameters = new ConnectionParameters(cassProperties.getHost(), cassProperties.getPort(), null, null, null, advancedParams);
        CassandraConnection cassClient = CassandraConnection.build(connectionParameters);
        assert cassClient != null;
    }

    @Test(expected = CassandraException.class)
    public void shouldNotConnect_Using_InvalidHost() {
        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V3, TestsConstants.CLUSTER_NAME, TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);
        cassProperties = AbstractTestCases.getCassandraProperties();
        ConnectionParameters connectionParameters = new ConnectionParameters(INVALID_HOST, cassProperties.getPort(), null, null, null, advancedParams);
        CassandraConnection.build(connectionParameters);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConnect_Using_NoHost() {
        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V3, TestsConstants.CLUSTER_NAME, TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);
        ConnectionParameters connectionParameters = new ConnectionParameters(null, cassProperties.getPort(), null, null, null, advancedParams);
        CassandraConnection.build(connectionParameters);
    }

    @Test(expected = CassandraException.class)
    public void shouldNotConnect_Using_InvalidPort() {
        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V3, TestsConstants.CLUSTER_NAME, TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);
        ConnectionParameters connectionParameters = new ConnectionParameters(cassProperties.getHost(), generateInvalidPort(cassProperties.getPort()), null, null, null, advancedParams);
        CassandraConnection.build(connectionParameters);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConnect_Using_NoPort() {
        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V3, TestsConstants.CLUSTER_NAME, TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);
        ConnectionParameters connectionParameters = new ConnectionParameters(cassProperties.getHost(), null, null, null, null, advancedParams);
        CassandraConnection.build(connectionParameters);
    }

    private String generateInvalidPort(String port) {
        Integer intPort = Integer.parseInt(port) - 1;
        return String.valueOf(intPort);
    }
}

/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.system;

import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import org.junit.Test;
import org.mule.api.ConnectionException;
import org.mule.modules.cassandradb.api.CassandraClient;
import org.mule.modules.cassandradb.automation.util.PropertiesLoaderUtil;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.configurations.AdvancedConnectionParameters;
import org.mule.modules.cassandradb.configurations.ConnectionParameters;
import org.mule.modules.cassandradb.utils.CassandraConfig;
import org.mule.tools.devkit.ctf.exceptions.ConfigurationLoadingFailedException;

public class ConfigTestCases  {

    private static CassandraConfig cassConfig;

    private static String INVALID_HOST = "INVALID_HOST";

    public static CassandraConfig getClientConfig() throws ConfigurationLoadingFailedException {
        //load required properties
        CassandraConfig cassConfig = PropertiesLoaderUtil.resolveCassandraConnectionProps();
        assert cassConfig != null;
        return cassConfig;
    }

    @Test
    public void shouldConnect_Using_BasicParams() throws Exception {
        //given
        cassConfig = getClientConfig();
        //when
        CassandraClient cassClient = CassandraClient.buildCassandraClient(new ConnectionParameters(cassConfig.getHost(), cassConfig.getPort(),null, null, null, null));
        //then
        assert cassClient != null;
    }

    @Test
    public void shouldConnect_Using_AdvancedParams() throws Exception {
        //given
        cassConfig = getClientConfig();

        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V3, TestsConstants.CLUSTER_NAME, cassConfig.getNodes(), TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);

        //when
        CassandraClient cassClient = CassandraClient.buildCassandraClient(new ConnectionParameters(null, null, null,  null, null, advancedParams));
        //then
        assert cassClient != null;
    }

    @Test(expected = ConnectionException.class)
    public void shouldNotConnect_Using_InvalidHost() throws ConnectionException, ConfigurationLoadingFailedException {
        //given
        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V3, TestsConstants.CLUSTER_NAME, null, TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);
        cassConfig = getClientConfig();
        //when
        CassandraClient.buildCassandraClient(new ConnectionParameters(INVALID_HOST, cassConfig.getPort(), null, null, null, advancedParams));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConnect_Using_NoHost() throws ConnectionException, ConfigurationLoadingFailedException {
        //given
        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V3, TestsConstants.CLUSTER_NAME, null, TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);
        cassConfig = getClientConfig();
        //when
        CassandraClient.buildCassandraClient(new ConnectionParameters(null, cassConfig.getPort(), null, null, null, advancedParams));
    }

    @Test(expected = ConnectionException.class)
    public void shouldNotConnect_Using_InvalidPort() throws ConnectionException, ConfigurationLoadingFailedException {
        //given
        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V3, TestsConstants.CLUSTER_NAME, null, TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);
        cassConfig = getClientConfig();
        //when
        CassandraClient.buildCassandraClient(new ConnectionParameters(cassConfig.getHost(), generateInvalidPort(cassConfig.getPort()), null, null, null, advancedParams));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConnect_Using_NoPort() throws ConnectionException, ConfigurationLoadingFailedException {
        //given
        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V3, TestsConstants.CLUSTER_NAME, null, TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);
        cassConfig = getClientConfig();
        //when
        CassandraClient.buildCassandraClient(new ConnectionParameters(cassConfig.getHost(), null, null, null, null, advancedParams));
    }

    private String generateInvalidPort(String port) {
        Integer intPort = Integer.parseInt(port) - 1;
        return String.valueOf(intPort);
    }
}

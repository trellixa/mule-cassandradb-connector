/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.system;

import org.junit.Ignore;
import org.mule.api.ConnectionException;
import org.mule.modules.cassandradb.automation.util.PropertiesLoaderUtil;
import org.junit.Test;

import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import org.mule.modules.cassandradb.api.CassandraClient;
import org.mule.modules.cassandradb.configurations.AdvancedConnectionParameters;
import org.mule.modules.cassandradb.configurations.ConnectionParameters;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.utils.CassandraConfig;
import org.mule.tools.devkit.ctf.exceptions.ConfigurationLoadingFailedException;

public class ConfigTestCases  {
    
    private static CassandraConfig cassConfig;

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
        CassandraClient cassClient = CassandraClient.buildCassandraClient(new ConnectionParameters(cassConfig.getNodes(), null, null, null, null));
        //then
        assert cassClient != null;
    }

    @Test
    public void shouldConnect_Using_AdvancedParams() throws Exception {
        //given
        AdvancedConnectionParameters advancedParams = new AdvancedConnectionParameters(ProtocolVersion.V3, TestsConstants.CLUSTER_NAME, TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);
        cassConfig = getClientConfig();
        //when
        CassandraClient cassClient = CassandraClient.buildCassandraClient(new ConnectionParameters(cassConfig.getNodes(), null, null, null, advancedParams));
        //then
        assert cassClient != null;
    }
}

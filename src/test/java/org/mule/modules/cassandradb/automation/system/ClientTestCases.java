/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.system;

import com.datastax.driver.core.*;
import org.junit.Before;
import org.junit.Test;
import org.mule.api.ConnectionException;
import org.mule.modules.cassandradb.api.CassandraClient;
import org.mule.modules.cassandradb.automation.util.PropertiesLoaderUtil;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.configurations.AdvancedConnectionParameters;
import org.mule.modules.cassandradb.configurations.ConnectionParameters;
import org.mule.modules.cassandradb.utils.CassandraConfig;
import org.mule.tools.devkit.ctf.exceptions.ConfigurationLoadingFailedException;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ClientTestCases {

    private Cluster.Builder clusterBuilder;
    private ConnectionParameters connectionParameters;
    private AdvancedConnectionParameters advancedConnectionParameters;
    private Cluster cluster;
    private Session session;
    private CassandraConfig cassandraConfig;

    @Before
    public void init() throws ConfigurationLoadingFailedException {
        clusterBuilder = Cluster.builder();

        cassandraConfig = getClientConfig();
    }

    @Test
    public void testConnectionWithAdvancedParameters() throws ConnectionException {
        advancedConnectionParameters = new AdvancedConnectionParameters(ProtocolVersion.V3,
                TestsConstants.CLUSTER_NAME, cassandraConfig.getClusterNodes(),
                TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);

        connectionParameters = new ConnectionParameters(null, null,
                null, null, null, advancedConnectionParameters);

        CassandraClient.connectWithAdvancedParams(connectionParameters, clusterBuilder);

        cluster = clusterBuilder.build();

        session = cluster.connect();

        assertThat(cluster.getMetadata().getAllHosts().size() > 1, is(true));
    }

    @Test
    public void testConnectionWithBasicParameters() throws ConnectionException {
        advancedConnectionParameters = new AdvancedConnectionParameters(ProtocolVersion.V3,
                TestsConstants.CLUSTER_NAME, null,
                TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);

        connectionParameters = new ConnectionParameters(cassandraConfig.getHost(), cassandraConfig.getPort(),
                null, null, null, advancedConnectionParameters);

        CassandraClient.connectWithBasicParams(connectionParameters, clusterBuilder);

        cluster = clusterBuilder.build();

        session = cluster.connect();

        List<Host> hosts = (List<Host>) session.getState().getConnectedHosts();

        assertThat(hosts.size() == 1, is(true));
    }

    @Test
    public void testConnectionWithBothBasicAndAdvancedParameters() throws ConnectionException {
        advancedConnectionParameters = new AdvancedConnectionParameters(ProtocolVersion.V3,
                TestsConstants.CLUSTER_NAME, cassandraConfig.getClusterNodes(),
                TestsConstants.MAX_WAIT, ProtocolOptions.Compression.NONE, false);

        connectionParameters = new ConnectionParameters(cassandraConfig.getHost(), cassandraConfig.getPort(),
                null, null, null, advancedConnectionParameters);

        CassandraClient.connectWithBasicParams(connectionParameters, clusterBuilder);

        cluster = clusterBuilder.build();

        session = cluster.connect();

        assertThat(cluster.getMetadata().getAllHosts().size() > 1, is(true));
    }

    private static CassandraConfig getClientConfig() throws ConfigurationLoadingFailedException {
        //load required properties

        return PropertiesLoaderUtil.resolveCassandraConnectionProps();
    }
}

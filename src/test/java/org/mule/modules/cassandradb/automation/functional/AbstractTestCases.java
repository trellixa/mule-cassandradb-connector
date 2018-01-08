package org.mule.modules.cassandradb.automation.functional;


import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;
import org.mule.modules.cassandradb.api.CQLQueryInput;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.automation.util.CassandraProperties;
import org.mule.modules.cassandradb.automation.util.PropertiesLoaderUtil;
import org.mule.modules.cassandradb.internal.config.CassandraConfig;
import org.mule.modules.cassandradb.internal.connection.BasicAuthConnectionProvider;
import org.mule.modules.cassandradb.internal.connection.CassandraConnection;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.modules.cassandradb.internal.metadata.CassandraMetadata;
import org.mule.modules.cassandradb.internal.service.CassandraService;
import org.mule.modules.cassandradb.internal.service.CassandraServiceImpl;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.tck.junit4.rule.SystemProperty;
import org.mule.tck.util.TestConnectivityUtils;
import org.mule.test.runner.ArtifactClassLoaderRunnerConfig;

import java.io.IOException;

@ArtifactClassLoaderRunnerConfig(
        exportPluginClasses = {
                CassandraError.class, CassandraService.class,
                CassandraConnection.class, CassandraProperties.class,
                CassandraConfig.class, CQLQueryInput.class, CassandraMetadata.class,
                BasicAuthConnectionProvider.class
        }
)
public abstract class AbstractTestCases extends MuleArtifactFunctionalTestCase {

    private static final String FLOW_CONFIG_LOCATION = "src/test/resources/automation-test-flows.xml";
    protected static final int SLEEP_DURATION = 2000;

    private CassandraConnection cassandraConnection;
    private CassandraService cassandraService;
    private CassandraProperties cassandraProperties;
    private CassandraMetadata cassandraMetadata;

    @Rule
    public SystemProperty rule2 = TestConnectivityUtils.disableAutomaticTestConnectivity();

    @Override
    public int getTestTimeoutSecs() {
        return 999999;
    }

    @Override
    protected String[] getConfigFiles() {
        return new String[] {
                FLOW_CONFIG_LOCATION
        };
    }

    @Before
    public void initialSetup() throws ConnectionException {
        cassandraProperties = getCassandraProperties();
        BasicAuthConnectionProvider basicAuthConnectionProvider = new BasicAuthConnectionProvider();
        basicAuthConnectionProvider.setHost(cassandraProperties.getHost());
        basicAuthConnectionProvider.setPort(cassandraProperties.getPort());
        CassandraConnection cassandraConnection = basicAuthConnectionProvider.connect();
        cassandraMetadata = new CassandraMetadata(cassandraConnection);
        cassandraService = new CassandraServiceImpl(new CassandraConfig(), cassandraConnection);
        assert cassandraConnection != null;
        //setup db env
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(cassandraProperties.getKeyspace());
        cassandraService.createKeyspace(keyspaceInput);
    }

    @After
    public void tearDown() {
        cassandraService.dropKeyspace(cassandraProperties.getKeyspace());
    }

    public static CassandraProperties getCassandraProperties() {
        //load required properties
        CassandraProperties cassProperties = null;
        try {
            cassProperties = PropertiesLoaderUtil.resolveCassandraConnectionProps();
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert cassProperties != null;
        return cassProperties;
    }

    public CassandraService getCassandraService() {
        return cassandraService;
    }

    public CassandraConnection getCassandraConnection() {
        return cassandraConnection;
    }

    public KeyspaceMetadata getKeyspaceMetadata(String keyspaceName) {
        return cassandraMetadata.getKeyspaceMetadata(keyspaceName);
    }

    public TableMetadata fetchTableMetadata(String keyspaceName, String tableName) {
        return cassandraMetadata.getTableMetadata(keyspaceName, tableName);
    }

    public CassandraMetadata getCassandraMetadata() {
        return cassandraMetadata;
    }

    protected String getKeyspaceFromProperties() {
        return getCassandraProperties().getKeyspace();
    }
}


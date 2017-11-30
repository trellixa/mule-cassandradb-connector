package org.mule.modules.cassandradb.automation.functional;


import com.datastax.driver.core.KeyspaceMetadata;
import org.junit.After;
import org.junit.Before;
import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.automation.util.CassandraProperties;
import org.mule.modules.cassandradb.automation.util.PropertiesLoaderUtil;
import org.mule.modules.cassandradb.internal.config.CassandraConfig;
import org.mule.modules.cassandradb.internal.connection.CassandraConnection;
import org.mule.modules.cassandradb.internal.connection.ConnectionParameters;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.modules.cassandradb.internal.service.CassandraService;
import org.mule.modules.cassandradb.internal.service.CassandraServiceImpl;
import org.mule.runtime.extension.api.error.ErrorTypeDefinition;
import org.mule.tck.junit4.matcher.ErrorTypeMatcher;
import org.mule.test.runner.ArtifactClassLoaderRunnerConfig;

import java.io.IOException;

@ArtifactClassLoaderRunnerConfig(
        exportPluginClasses = {CassandraError.class, CassandraService.class,
                CassandraConnection.class, CassandraProperties.class,
                CassandraConfig.class }
)
public class AbstractTestCases extends MuleArtifactFunctionalTestCase {

    private static final String FLOW_CONFIG_LOCATION = "src/test/resources/automation-test-flows.xml";

    private CassandraConnection cassandraConnection;
    private CassandraService cassandraService;
    private CassandraProperties cassandraProperties;

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
    public void initialSetup() {
        cassandraProperties = getCassandraProperties();
        ConnectionParameters connectionParameters = new ConnectionParameters(cassandraProperties.getHost(), cassandraProperties.getPort(), null, null, null, null);
        cassandraConnection = CassandraConnection.build(connectionParameters);
        cassandraService = new CassandraServiceImpl(new CassandraConfig(), cassandraConnection);
        assert cassandraConnection != null;
        assert cassandraService != null;
        //setup db env
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(cassandraProperties.getKeyspace());

        cassandraService.createKeyspace(keyspaceInput);
    }

    @After
    public void tearDown() {
        cassandraService.dropKeyspace(cassandraProperties.getKeyspace());
    }

    boolean createKeyspace(final CreateKeyspaceInput keyspaceInput) throws Exception {
        return (boolean) flowRunner("createKeyspace-flow")
                .withPayload(keyspaceInput)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    void createKeyspaceExpException(final CreateKeyspaceInput keyspaceInput, ErrorTypeDefinition errorType) throws Exception {
        flowRunner("createKeyspace-flow")
                .withPayload(keyspaceInput)
                .runExpectingException(ErrorTypeMatcher.errorType(errorType));
    }

    boolean dropKeyspace(String keyspaceName) throws Exception {
        return (boolean) flowRunner("deleteKeyspace-flow")
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    boolean createTable(CreateTableInput basicCreateTableInput) throws Exception {
        return (boolean) flowRunner("createTable-flow")
                .withPayload(basicCreateTableInput)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    boolean dropTable(String tableName, String keyspaceName) throws Exception {
        return (boolean) flowRunner("dropTable-flow")
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
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
        return getCassandraConnection().getCluster().getMetadata().getKeyspace(keyspaceName);
    }
}

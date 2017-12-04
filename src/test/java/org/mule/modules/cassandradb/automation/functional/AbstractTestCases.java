package org.mule.modules.cassandradb.automation.functional;


import com.datastax.driver.core.KeyspaceMetadata;
import org.junit.After;
import org.junit.Before;
import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;
import org.mule.modules.cassandradb.api.AlterColumnInput;
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
import java.util.List;
import java.util.Map;

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

    boolean addNewColumn(String tableName, String keyspaceName, AlterColumnInput alterColumnInput) throws Exception {
        return (boolean) flowRunner("addColumn-flow")
                .withPayload(alterColumnInput)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

     void addNewColumnExpException(String tableName, String keyspaceName, AlterColumnInput alterColumnInput) throws Exception {
         flowRunner("addColumn-flow")
                 .withPayload(alterColumnInput)
                 .withVariable("tableName", tableName)
                 .withVariable("keyspaceName", keyspaceName)
                 .runExpectingException(ErrorTypeMatcher.errorType(CassandraError.UNKNOWN));
    }

    boolean dropColumn(String tableName, String keyspaceName, String column) throws Exception {
        return (boolean) flowRunner("dropColumn-flow")
                .withPayload(column)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    void dropColumnExpException(String tableName, String keyspaceName, String column) throws Exception {
        flowRunner("dropColumn-flow")
                .withPayload(column)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .runExpectingException(ErrorTypeMatcher.errorType(CassandraError.UNKNOWN));
    }

    boolean renameColumn(String tableName, String keyspaceName, String column, String newColumn) throws Exception {
        return (boolean) flowRunner("renameColumn-flow")
                .withPayload(column)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .withVariable("newColumnName", newColumn)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    void renameColumnExpException(String tableName, String keyspaceName, String column, String newColumn) throws Exception {
        flowRunner("renameColumn-flow")
                .withPayload(column)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .withVariable("newColumnName", newColumn)
                .runExpectingException(ErrorTypeMatcher.errorType(CassandraError.UNKNOWN));
    }

    boolean changeColumnType(String tableName, String keyspaceName, AlterColumnInput input) throws Exception {
        return (boolean) flowRunner("changeColumnType-flow")
                .withPayload(input)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    void changeColumnTypeExpException(String tableName, String keyspaceName, AlterColumnInput input) throws Exception {
        flowRunner("changeColumnType-flow")
                .withPayload(input)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .runExpectingException(ErrorTypeMatcher.errorType(CassandraError.UNKNOWN));
    }

    List<String> getTableNamesFromKeyspace(String keyspaceName) throws Exception {
        return (List<String>) flowRunner("getTableNamesFromKeyspace-flow")
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    void insert(String tableName, String keyspaceName, Map<String, Object> entity) throws Exception {
        flowRunner("insert-flow")
                .withPayload(entity)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    void insertExpError(String tableName, String keyspaceName, Map<String, Object> entity) throws Exception {
        flowRunner("insert-flow")
                .withPayload(entity)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .runExpectingException(ErrorTypeMatcher.errorType(CassandraError.UNKNOWN));
    }

    protected void update(String tableName, String keyspaceName, Map<String, Object> entityToUpdate) throws Exception {
        flowRunner("update-flow")
                .withPayload(entityToUpdate)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void updateExpException(String tableName, String keyspaceName, Map<String, Object> entityToUpdate) throws Exception {
        flowRunner("update-flow")
                .withPayload(entityToUpdate)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .runExpectingException(ErrorTypeMatcher.errorType(CassandraError.UNKNOWN));
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

package org.mule.modules.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;
import org.mule.modules.cassandradb.api.AlterColumnInput;
import org.mule.modules.cassandradb.api.CQLQueryInput;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.runtime.extension.api.error.ErrorTypeDefinition;
import org.mule.tck.junit4.rule.SystemProperty;
import org.mule.tck.util.TestConnectivityUtils;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.KEYSPACE_DUMMY;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getAutomationCredentialsProperties;
import static org.mule.tck.junit4.matcher.ErrorTypeMatcher.errorType;

public abstract class AbstractTestCases extends MuleArtifactFunctionalTestCase {

    @Rule
    public SystemProperty rule2 = TestConnectivityUtils.disableAutomaticTestConnectivity();

    private static final String FLOW_CONFIG_LOCATION = "src/test/resources/automation-test-flows.xml";
    protected static final int SLEEP_DURATION = 2000;
    Properties cassandraProperties;
    protected String testKeyspace;

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
    public void initialSetup() throws Exception {
        cassandraProperties  = getAutomationCredentialsProperties();
        testKeyspace = KEYSPACE_DUMMY;
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(testKeyspace);
        createKeyspace(keyspaceInput);
    }

    @After
    public void finalTearDown() throws Exception {
        dropKeyspace(testKeyspace);
    }

    protected void createKeyspace(final CreateKeyspaceInput keyspaceInput) throws Exception {
        flowRunner("createKeyspace-flow")
                .withPayload(keyspaceInput)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void dropKeyspace(final String keyspaceInput) throws Exception {
        flowRunner("dropKeyspace-flow")
                .withPayload(keyspaceInput)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void createTable(CreateTableInput basicCreateTableInput) throws Exception {
        flowRunner("createTable-flow")
                .withPayload(basicCreateTableInput)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void dropTable(String keyspaceName, String tableName) throws Exception {
        flowRunner("dropTable-flow")
                .withPayload(tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void addNewColumn(String tableName, String keyspaceName, AlterColumnInput alterColumnInput) throws Exception {
        flowRunner("addColumn-flow")
                .withPayload(alterColumnInput)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void changeColumnType(String tableName, String keyspaceName, AlterColumnInput input) throws Exception {
        flowRunner("changeColumnType-flow")
                .withPayload(input)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void createKeyspaceExpException(final CreateKeyspaceInput keyspaceInput, ErrorTypeDefinition errorType) throws Exception {
        flowRunner("createKeyspace-flow")
                .withPayload(keyspaceInput)
                .runExpectingException(errorType(errorType));
    }

    protected void deleteColumnsValue(String tableName, String keyspaceName, List<String> entities, Map<String, Object> whereClause) throws Exception {
        flowRunner("deleteColumnsValue-flow")
                .withPayload(whereClause)
                .withVariable("entities", entities)
                .withVariable("keyspaceName", keyspaceName)
                .withVariable("tableName", tableName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void deleteRows(String tableName, String keyspaceName, Map<String, Object> payloadColumnsAndFilters) throws Exception {
        flowRunner("deleteRows-flow")
                .withPayload(payloadColumnsAndFilters)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void dropColumn(String tableName, String keyspaceName, String column) throws Exception {
         flowRunner("dropColumn-flow")
                .withPayload(column)
                .withVariable("table", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected List<Map<String, Object>> executeCQLQuery(CQLQueryInput query) throws Exception {
        return (List<Map<String, Object>>) flowRunner("executeCQLQuery-flow")
                .withPayload(query)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected List<String> getTableNamesFromKeyspace(String keyspaceName) throws Exception {
        return (List<String>) flowRunner("getTableNamesFromKeyspace-flow")
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void insert(String keyspaceName, String tableName, Map<String, Object> entity) throws Exception {
        flowRunner("insert-flow")
                .withPayload(entity)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected List<Map<String,Object>> select(final String query, List<Object> parameters) throws Exception {
        return (List) flowRunner("select-flow")
                .withPayload(query)
                .withVariable("parameters", parameters)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void renameColumn(String tableName, String keyspaceName, String column, String newColumn) throws Exception {
        flowRunner("renameColumn-flow")
                .withPayload(column)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .withVariable("newColumnName", newColumn)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void update(String tableName, String keyspaceName, Map<String, Object> entityToUpdate) throws Exception {
        flowRunner("update-flow").withPayload(entityToUpdate)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }
}


/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.tck.junit4.rule.SystemProperty;
import org.mule.tck.util.TestConnectivityUtils;

import java.util.Properties;

import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.KEYSPACE_DUMMY;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getAutomationCredentialsProperties;

public abstract class AbstractTestCases extends MuleArtifactFunctionalTestCase {

    @Rule
    public SystemProperty rule2 = TestConnectivityUtils.disableAutomaticTestConnectivity();

    private static final String FLOW_CONFIG_LOCATION = "src/test/resources/flows/automation-test-flows.xml";
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
}


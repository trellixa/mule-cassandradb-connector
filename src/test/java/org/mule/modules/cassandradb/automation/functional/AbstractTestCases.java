package org.mule.modules.cassandradb.automation.functional;


import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.runtime.extension.api.error.ErrorTypeDefinition;
import org.mule.tck.junit4.matcher.ErrorTypeMatcher;
import org.mule.test.runner.ArtifactClassLoaderRunnerConfig;

@ArtifactClassLoaderRunnerConfig(
        exportPluginClasses = {CassandraError.class}
)
public class AbstractTestCases extends MuleArtifactFunctionalTestCase {

    private static final String FLOW_CONFIG_LOCATION = "src/test/resources/automation-test-flows.xml";

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

    protected boolean createKeyspace(final CreateKeyspaceInput keyspaceInput) throws Exception {
        return (boolean) flowRunner("createKeyspace-flow")
                .withPayload(keyspaceInput)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void createKeyspaceExpException(final CreateKeyspaceInput keyspaceInput, ErrorTypeDefinition errorType) throws Exception {
        flowRunner("createKeyspace-flow")
                .withPayload(keyspaceInput)
                .runExpectingException(ErrorTypeMatcher.errorType(errorType));
    }

    protected boolean dropKeyspace(String keyspaceName) throws Exception {
        return (boolean) flowRunner("deleteKeyspace-flow")
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }
}

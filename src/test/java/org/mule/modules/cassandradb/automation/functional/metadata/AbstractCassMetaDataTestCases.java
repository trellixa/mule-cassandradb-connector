package org.mule.modules.cassandradb.automation.functional.metadata;

import org.mule.modules.cassandradb.CassandraDBConnector;
import org.mule.modules.cassandradb.api.CassandraClient;
import org.mule.modules.cassandradb.automation.functional.TestDataBuilder;
import org.mule.modules.cassandradb.configurations.ConnectionParameters;
import org.mule.modules.cassandradb.metadata.CreateKeyspaceInput;
import org.mule.modules.cassandradb.automation.util.ConstantsTest;
import org.mule.modules.cassandradb.automation.util.PropertiesLoaderUtil;
import org.mule.modules.cassandradb.utils.CassandraConfig;
import org.mule.modules.cassandradb.utils.CassandraDBException;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mule.api.ConnectionException;
import org.mule.tools.devkit.ctf.exceptions.ConfigurationLoadingFailedException;
import org.mule.tools.devkit.ctf.junit.AbstractMetaDataTestCase;

import java.util.List;

public abstract class AbstractCassMetaDataTestCases extends AbstractMetaDataTestCase<CassandraDBConnector> {

    public AbstractCassMetaDataTestCases(@NotNull List<String> metadataIds, @NotNull Class<?> categoryClass, Class<CassandraDBConnector> connectorClass) {
        super(metadataIds, categoryClass, connectorClass);
    }

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;

    @BeforeClass
    public static void initialSetup() throws ConfigurationLoadingFailedException, ConnectionException, CassandraDBException, InterruptedException {
        cassConfig = getClientConfig();
        //get instance of cass client based on the configs
        cassClient = CassandraClient.buildCassandraClient(new ConnectionParameters(cassConfig.getHost(), cassConfig.getPort(), null, null, null, null));
        assert cassClient != null;
        //setup db env
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(cassConfig.getKeyspace());
        cassClient.createKeyspace(keyspaceInput);
        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getPrimaryKey(), cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_2));
        //required delay to make sure the setup is ok
        Thread.sleep(5000);
    }

    @AfterClass
    public static void tearDown() {
        cassClient.dropTable(ConstantsTest.TABLE_NAME_2, cassConfig.getKeyspace());
        cassClient.dropKeyspace(cassConfig.getKeyspace());
    }

    public static CassandraConfig getClientConfig() throws ConfigurationLoadingFailedException {
        //load required properties
        CassandraConfig cassConfig = PropertiesLoaderUtil.resolveCassandraConnectionProps();
        assert cassConfig != null;
        return cassConfig;
    }
}

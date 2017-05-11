package com.mulesoft.mule.cassandradb.automation.functional.metadata;

import com.mulesoft.mule.cassandradb.CassandraDBConnector;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.automation.functional.TestDataBuilder;
import com.mulesoft.mule.cassandradb.configurations.ConnectionParameters;
import com.mulesoft.mule.cassandradb.metadata.CreateKeyspaceInput;
import com.mulesoft.mule.cassandradb.util.ConstantsTest;
import com.mulesoft.mule.cassandradb.util.PropertiesLoaderUtil;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mule.api.ConnectionException;
import org.mule.tools.devkit.ctf.exceptions.ConfigurationLoadingFailedException;
import org.mule.tools.devkit.ctf.junit.AbstractMetaDataTestCase;

import java.util.List;

public abstract class AbstractCassMetaDataTestCase extends AbstractMetaDataTestCase<CassandraDBConnector> {

    public AbstractCassMetaDataTestCase(@NotNull List<String> metadataIds, @NotNull Class<?> categoryClass, Class<CassandraDBConnector> connectorClass) {
        super(metadataIds, categoryClass, connectorClass);
    }

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;

    @BeforeClass
    public static void initialSetup() throws ConfigurationLoadingFailedException, ConnectionException, CassandraDBException {
        cassConfig = getClientConfig();
        //get instance of cass client based on the configs
        cassClient = CassandraClient.buildCassandraClient(new ConnectionParameters(cassConfig.getHost(), cassConfig.getPort(), null, null, null, null));
        assert cassClient != null;
        //setup db env
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(cassConfig.getKeyspace());
        cassClient.createKeyspace(keyspaceInput);
        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getPrimaryKey(), cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_2));
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

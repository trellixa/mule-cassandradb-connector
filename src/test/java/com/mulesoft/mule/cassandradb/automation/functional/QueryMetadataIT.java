package com.mulesoft.mule.cassandradb.automation.functional;

import com.mulesoft.mule.cassandradb.CassandraDBConnector;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.util.PropertiesLoaderUtil;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import com.mulesoft.mule.cassandradb.utils.Constants;
import org.junit.*;
import org.mule.api.ConnectionException;
import org.mule.common.Result;
import org.mule.common.metadata.MetaData;
import org.mule.common.metadata.MetaDataKey;
import org.mule.tools.devkit.ctf.exceptions.ConfigurationLoadingFailedException;
import org.mule.tools.devkit.ctf.junit.AbstractTestCase;
import org.mule.tools.devkit.ctf.junit.MetaDataTest;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class QueryMetadataIT extends AbstractTestCase<CassandraDBConnector> {

    private static CassandraClient cassClient;

    public QueryMetadataIT() {
        super(CassandraDBConnector.class);
    }

    @BeforeClass
    public static void setup() throws ConnectionException, CassandraDBException, IOException, ConfigurationLoadingFailedException {
        //load required properties
        CassandraConfig cassConfig = PropertiesLoaderUtil.resolveCassandraConnectionProps();
        assert cassConfig != null;
        //get instance of cass client based on the configs
        cassClient = CassandraClient.buildCassandraClient(cassConfig.getHost(), cassConfig.getPort(), null, null, null);
        assert cassClient != null;

        //setup db env
        cassClient.createKeyspace(Constants.KEYSPACE_NAME, null);
        cassClient.createTable(Constants.TABLE_NAME, Constants.KEYSPACE_NAME, null);
    }

    @AfterClass
    public static void tearDown() throws CassandraDBException, ConnectionException {
        cassClient.dropKeyspace(Constants.KEYSPACE_NAME);
        cassClient.dropTable(Constants.TABLE_NAME, Constants.KEYSPACE_NAME);
    }

    @Test
    @MetaDataTest
    public void shouldRetrieveInputMetadata() {
        //given
        MetaDataKey metaDataKey = buildMetadataKey();
        assertNotNull(metaDataKey);
        assertEquals(metaDataKey.getDisplayName(), Constants.TABLE_NAME);

        //when
        Result<MetaData> tableMetadataResult = getDispatcher().fetchMetaData(metaDataKey);

        //then
        assertTrue(Result.Status.SUCCESS.equals(tableMetadataResult.getStatus()));
        assertNotNull(tableMetadataResult.get().getPayload());
    }

    private MetaDataKey buildMetadataKey() {
        //when
        Result<List<MetaDataKey>> metaDataKeysResult = getDispatcher().fetchMetaDataKeys();
        assertTrue(Result.Status.SUCCESS.equals(metaDataKeysResult.getStatus()));
        List<MetaDataKey> metaDataKeys = metaDataKeysResult.get();

        //then
        assertFalse(metaDataKeys.isEmpty());
        for (MetaDataKey key : metaDataKeys) {
            if (key.getId().equals(Constants.TABLE_NAME)) {
                return key;
            }
        }

        return null;
    }
}

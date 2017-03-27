package com.mulesoft.mule.cassandradb.automation.functional;

import com.mulesoft.mule.cassandradb.CassandraDBConnector;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import com.mulesoft.mule.cassandradb.utils.Constants;
import org.junit.*;
import org.mule.api.ConnectionException;
import org.mule.common.Result;
import org.mule.common.metadata.MetaData;
import org.mule.common.metadata.MetaDataKey;
import org.mule.tools.devkit.ctf.junit.AbstractTestCase;
import org.mule.tools.devkit.ctf.junit.MetaDataTest;

import java.util.List;

import static org.junit.Assert.*;

public class QueryMetadataIT extends AbstractTestCase<CassandraDBConnector> {

    public QueryMetadataIT() {
        super(CassandraDBConnector.class);
    }

    @BeforeClass
    public static void before() throws ConnectionException, CassandraDBException {
        CassandraClient cassandraClient = CassandraClient.buildCassandraClient("127.0.0.1", 9042, null, null, null);
        cassandraClient.createKeyspace(Constants.KEYSPACE_NAME, null);
        cassandraClient.createTable(Constants.TABLE_NAME, Constants.KEYSPACE_NAME, null);
    }

    @AfterClass
    public void tearDown() throws CassandraDBException {
        getConnector().dropTable(Constants.TABLE_NAME, Constants.KEYSPACE_NAME);
        getConnector().dropKeyspace(Constants.KEYSPACE_NAME);
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
        assertNotNull(tableMetadataResult.get());
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

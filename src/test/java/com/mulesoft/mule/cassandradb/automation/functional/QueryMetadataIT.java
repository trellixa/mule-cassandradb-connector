/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional;

import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.metadata.ColumnInput;
import com.mulesoft.mule.cassandradb.metadata.CreateKeyspaceInput;
import com.mulesoft.mule.cassandradb.metadata.CreateTableInput;
import com.mulesoft.mule.cassandradb.util.ConstantsTest;
import com.mulesoft.mule.cassandradb.util.PropertiesLoaderUtil;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import org.junit.*;
import org.mule.common.Result;
import org.mule.common.metadata.MetaData;
import org.mule.common.metadata.MetaDataKey;
import org.mule.tools.devkit.ctf.junit.MetaDataTest;

import java.util.List;

import static org.junit.Assert.*;

public class QueryMetadataIT extends BaseTestCases {

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;

    @BeforeClass
    public static void setup() throws Exception {
        //load required properties
        cassConfig = PropertiesLoaderUtil.resolveCassandraConnectionProps();
        assert cassConfig != null;
        //get instance of cass client based on the configs
        cassClient = CassandraClient.buildCassandraClient(cassConfig.getHost(), cassConfig.getPort(), null, null, null);
        assert cassClient != null;

        //setup db env
        List<ColumnInput> columns = TestDataBuilder.getPrimaryKey();
        CreateTableInput input = new CreateTableInput();
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();

        input.setColumns(columns);
        input.setKeyspaceName(cassConfig.getKeyspace());
        input.setTableName(ConstantsTest.TABLE_NAME);

        keyspaceInput.setKeyspaceName(cassConfig.getKeyspace());
        cassClient.createTable(input);
        cassClient.createKeyspace(keyspaceInput);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        cassClient.dropTable(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace());
    }

    @Test
    @MetaDataTest
    public void shouldRetrieveInputMetadata() {
        //given
        MetaDataKey metaDataKey = buildMetadataKey();
        assertNotNull(metaDataKey);
        assertEquals(metaDataKey.getDisplayName(), ConstantsTest.TABLE_NAME);

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
            if (key.getId().equals(ConstantsTest.TABLE_NAME)) {
                return key;
            }
        }

        return null;
    }
}

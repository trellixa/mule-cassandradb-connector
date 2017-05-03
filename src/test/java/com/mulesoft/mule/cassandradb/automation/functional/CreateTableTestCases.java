/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional;

import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.metadata.CreateKeyspaceInput;
import com.mulesoft.mule.cassandradb.util.ConstantsTest;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateTableTestCases extends CassandraDBConnectorAbstractTestCase {

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;
    private final static String tableName1 = "tableName1";
    private final static String tableName2 = "tableName2";

    @BeforeClass
    public static void setup() throws Exception {
        cassConfig = getClientConfig();
        //get instance of cass client based on the configs
        cassClient = CassandraClient.buildCassandraClient(cassConfig.getHost(), cassConfig.getPort(), null, null, null);
        assert cassClient != null;

        //setup db env
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(cassConfig.getKeyspace());
        cassClient.createKeyspace(keyspaceInput);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        cassClient.dropTable(tableName1, cassConfig.getKeyspace());
        cassClient.dropTable(tableName2, cassConfig.getKeyspace());
        cassClient.dropTable(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace());
    }

    @Test
    public void testCreateTableWithSuccess() throws CassandraDBException {

        getConnector().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), cassConfig.getKeyspace(), tableName1));
    }

    @Test
    public void testCreateTableWithCompositePKWithSuccess() throws CassandraDBException {

        getConnector().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getCompositePrimaryKey(), cassConfig.getKeyspace(), tableName1));
    }


}

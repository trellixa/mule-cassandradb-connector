/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb;

import com.mulesoft.mule.cassandradb.automation.functional.TestDataBuilder;
import com.mulesoft.mule.cassandradb.configurations.BasicAuthConnectionStrategy;
import com.mulesoft.mule.cassandradb.metadata.*;
import com.mulesoft.mule.cassandradb.util.ConstantsTest;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.mule.api.ConnectionException;
import org.mule.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CassandraDBConnectorMockTest {

    private static CassandraDBConnector connector = new CassandraDBConnector();
    private static BasicAuthConnectionStrategy strategy = new BasicAuthConnectionStrategy();

    @BeforeClass
    public static void init() throws ConnectionException, CassandraDBException {
        strategy.setHost("127.0.0.1");
        strategy.setPort(9042);
        strategy.connect(null, null);

        connector.setBasicAuthConnectionStrategy(strategy);

        //create a keyspace to be further used
        CreateKeyspaceInput input = new CreateKeyspaceInput();
        input.setKeyspaceName(ConstantsTest.SECOND_KEYSPACE_NAME);
        connector.createKeyspace(input);
    }

    @AfterClass
    public static void tearDown() throws CassandraDBException {
        connector.dropKeyspace(ConstantsTest.SECOND_KEYSPACE_NAME);
        strategy.getCassandraClient().close();
    }

    @Test public void shouldCreateTableOnlyWithPrimaryKey() throws CassandraDBException {
        //create input
        List<ColumnInput> columns = new ArrayList<ColumnInput>();
        CreateTableInput input = new CreateTableInput();

        ColumnInput column = new ColumnInput();
        column.setPrimaryKey(true);
        column.setName(ConstantsTest.DUMMY_PARTITION_KEY);
        column.setType(ColumnType.TEXT);
        columns.add(column);

        input.setColumns(columns);
        input.setKeyspaceName(ConstantsTest.SECOND_KEYSPACE_NAME);
        input.setTableName(ConstantsTest.TABLE_NAME);
        //create a table
        connector.createTable(input);
        Assert.assertTrue(verifyTableCreation(ConstantsTest.TABLE_NAME, ConstantsTest.SECOND_KEYSPACE_NAME));
    }

    @Test
    public void shouldCreateTableWithMultipleColumns() throws CassandraDBException {

        //create input
        List<ColumnInput> columns = TestDataBuilder.getColumns();
        CreateTableInput input = new CreateTableInput();

        input.setColumns(columns);
        input.setKeyspaceName(ConstantsTest.SECOND_KEYSPACE_NAME);
        input.setTableName(ConstantsTest.TABLE_NAME);

        //create a table
        connector.createTable(input);
        Assert.assertTrue(verifyTableCreation(ConstantsTest.TABLE_NAME, ConstantsTest.SECOND_KEYSPACE_NAME));
    }

    @Test (expected = CassandraDBException.class)
    public void shouldFailCreateTableWithoutPrimaryKey() throws CassandraDBException {
        //create input
        List<ColumnInput> columns = new ArrayList<ColumnInput>();
        CreateTableInput input = new CreateTableInput();

        ColumnInput column = new ColumnInput();
        column.setName(ConstantsTest.VALID_COLUMN);
        column.setType(ColumnType.INT);
        columns.add(column);
        input.setColumns(columns);

        connector.createTable(input);
        Assert.assertTrue(verifyTableCreation(ConstantsTest.TABLE_NAME, ConstantsTest.SECOND_KEYSPACE_NAME));
    }

    @Test
    public void shouldDropTable() throws CassandraDBException {
        connector.dropTable(ConstantsTest.TABLE_NAME, ConstantsTest.SECOND_KEYSPACE_NAME);
        Assert.assertFalse(verifyTableCreation(ConstantsTest.TABLE_NAME, ConstantsTest.SECOND_KEYSPACE_NAME));
    }

    @Test
    public void shouldDescribeKeyspaces() throws CassandraDBException {
        CreateCQLQueryInput input = new CreateCQLQueryInput();
        input.setCqlQuery("SELECT * FROM system.schema_keyspaces;");
        List result = connector.executeCQLQuery(input);
        Assert.assertTrue(result.size() > 0);
    }

    /**
     * helper method used to verify is a table was created in a specific keyspace
     */
    private boolean verifyTableCreation(String tableToVerify, String keyspaceName) {
        List<String> tableNames = strategy.getCassandraClient().getTableNamesFromKeyspace(keyspaceName);
        return CollectionUtils.isNotEmpty(tableNames) && tableNames.contains(tableToVerify);
    }
}

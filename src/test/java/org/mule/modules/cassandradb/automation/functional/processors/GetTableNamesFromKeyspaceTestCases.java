package org.mule.modules.cassandradb.automation.functional.processors;

import org.junit.Test;
import org.mule.modules.cassandradb.automation.functional.TestDataBuilder;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.metadata.CreateKeyspaceInput;
import org.mule.modules.cassandradb.utils.CassandraDBException;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GetTableNamesFromKeyspaceTestCases extends CassandraAbstractTestCases {

    @Test
    public void testTableNamesFromLoggedInKeyspace() throws CassandraDBException {
        getConnector().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), cassConfig.getKeyspace(), TestsConstants.TABLE_NAME_1));
        getConnector().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), cassConfig.getKeyspace(), TestsConstants.TABLE_NAME_2));

        List<String> tableNames = getConnector().getTableNamesFromKeyspace(null);

        assertEquals(2, tableNames.size());
        assertTrue(tableNames.contains(TestsConstants.TABLE_NAME_1));
        assertTrue(tableNames.contains(TestsConstants.TABLE_NAME_2));

        cassClient.dropTable(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace());
        cassClient.dropTable(TestsConstants.TABLE_NAME_2, cassConfig.getKeyspace());
    }

    @Test
    public void testTableNamesFromCustomInKeyspace() throws CassandraDBException {
        String testTable = "testTable";
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(TestsConstants.KEYSPACE_NAME_3);

        getConnector().createKeyspace(keyspaceInput);

        getConnector().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), TestsConstants.KEYSPACE_NAME_3, TestsConstants.TABLE_NAME_1));
        getConnector().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), TestsConstants.KEYSPACE_NAME_3, TestsConstants.TABLE_NAME_2));
        getConnector().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), cassConfig.getKeyspace(), testTable));

        List<String> tableNames = getConnector().getTableNamesFromKeyspace(TestsConstants.KEYSPACE_NAME_3);

        assertEquals(2, tableNames.size());
        assertTrue(tableNames.contains(TestsConstants.TABLE_NAME_1));
        assertTrue(tableNames.contains(TestsConstants.TABLE_NAME_2));

        cassClient.dropKeyspace(TestsConstants.KEYSPACE_NAME_3);
    }
}

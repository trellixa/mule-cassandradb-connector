/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GetTableNamesFromKeyspaceTestCase extends AbstractTestCases {

    @Test
    public void testTableNamesFromLoggedInKeyspace() throws Exception {
        createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), testKeyspace, TestDataBuilder.TABLE_NAME_1));
        createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), testKeyspace, TestDataBuilder.TABLE_NAME_2));
        List<String> tableNames = getTableNamesFromKeyspace(testKeyspace);
        assertEquals(2, tableNames.size());
        assertTrue(tableNames.contains(TestDataBuilder.TABLE_NAME_1));
        assertTrue(tableNames.contains(TestDataBuilder.TABLE_NAME_2));
        dropTable(TestDataBuilder.TABLE_NAME_1, testKeyspace);
        dropTable(TestDataBuilder.TABLE_NAME_2, testKeyspace);
    }

    @Test
    public void testTableNamesFromCustomInKeyspace() throws Exception {
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(TestDataBuilder.KEYSPACE_NAME_3);
        createKeyspace(keyspaceInput);
        createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), TestDataBuilder.KEYSPACE_NAME_3, TestDataBuilder.TABLE_NAME_1));
        createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), TestDataBuilder.KEYSPACE_NAME_3, TestDataBuilder.TABLE_NAME_2));
        createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), testKeyspace, "testTable"));
        List<String> tableNames = getTableNamesFromKeyspace(TestDataBuilder.KEYSPACE_NAME_3);
        assertEquals(2, tableNames.size());
        assertTrue(tableNames.contains(TestDataBuilder.TABLE_NAME_1));
        assertTrue(tableNames.contains(TestDataBuilder.TABLE_NAME_2));
        dropKeyspace(TestDataBuilder.KEYSPACE_NAME_3);
    }
}

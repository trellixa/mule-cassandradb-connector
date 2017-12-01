/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.automation.util.TestsConstants;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.KEYSPACE_NAME_3;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.TABLE_NAME_2;

public class GetTableNamesFromKeyspaceTestCases extends AbstractTestCases {

    @Test
    public void testTableNamesFromLoggedInKeyspace() throws Exception {
        getCassandraService().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), getCassandraProperties().getKeyspace(), TABLE_NAME_1));
        getCassandraService().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), getCassandraProperties().getKeyspace(), TABLE_NAME_2));

        List<String> tableNames = getTableNamesFromKeyspace(null);

        assertEquals(2, tableNames.size());
        assertTrue(tableNames.contains(TABLE_NAME_1));
        assertTrue(tableNames.contains(TABLE_NAME_2));

        getCassandraService().dropTable(TABLE_NAME_1, getCassandraProperties().getKeyspace());
        getCassandraService().dropTable(TABLE_NAME_2, getCassandraProperties().getKeyspace());
    }

    @Test
    public void testTableNamesFromCustomInKeyspace() throws Exception {
        String testTable = "testTable";
        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(KEYSPACE_NAME_3);

        getCassandraService().createKeyspace(keyspaceInput);

        getCassandraService().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), KEYSPACE_NAME_3, TABLE_NAME_1));
        getCassandraService().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), KEYSPACE_NAME_3, TABLE_NAME_2));
        getCassandraService().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), getCassandraProperties().getKeyspace(), testTable));

        List<String> tableNames = getTableNamesFromKeyspace(KEYSPACE_NAME_3);

        assertEquals(2, tableNames.size());
        assertTrue(tableNames.contains(TABLE_NAME_1));
        assertTrue(tableNames.contains(TABLE_NAME_2));

        getCassandraService().dropKeyspace(KEYSPACE_NAME_3);
    }
}

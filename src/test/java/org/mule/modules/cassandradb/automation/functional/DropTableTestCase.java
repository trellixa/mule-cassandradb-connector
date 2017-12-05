/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.automation.util.TestsConstants;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DropTableTestCase extends AbstractTestCases {

    @Test
    public void testDropTable() throws Exception {
        String tableName = TestsConstants.TABLE_NAME_1;
        String keyspace = getKeyspaceFromProperties();
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), keyspace, tableName);
        getCassandraService().createTable(basicCreateTableInput);
        assertNotNull(getTableMetadata(tableName, keyspace));

        boolean result = dropTable(tableName, keyspace);
        assertTrue(result);

        assertNull(getTableMetadata(tableName, keyspace));
    }

    @Test
    public void testDropTableWithCompositePK() throws Exception {
        String tableName = TestsConstants.TABLE_NAME_2;
        String keyspace = getKeyspaceFromProperties();
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getCompositePrimaryKey(), keyspace, tableName);
        getCassandraService().createTable(basicCreateTableInput);
        assertNotNull(getTableMetadata(tableName, keyspace));

        boolean result = dropTable(tableName, keyspace);
        assertTrue(result);

        assertNull(getTableMetadata(tableName, keyspace));
    }

    private TableMetadata getTableMetadata(String tableName, String keyspaceName) {
        KeyspaceMetadata keyspaceMetadata = getKeyspaceMetadata(keyspaceName);
        return keyspaceMetadata.getTable(tableName);
    }
}

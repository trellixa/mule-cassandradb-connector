/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.automation.util.TestDataBuilder;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.TABLE_NAME_2;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getBasicCreateTableInput;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getCompositePrimaryKey;

public class DropTableTestCase extends AbstractTestCases {

    @Test
    public void testDropTable() throws Exception {
        String tableName = TABLE_NAME_1;
        String keyspace = getKeyspaceFromProperties();
        CreateTableInput basicCreateTableInput = getBasicCreateTableInput(TestDataBuilder.getColumns(), keyspace, tableName);
        getCassandraService().createTable(basicCreateTableInput);
        assertNotNull(fetchTableMetadata(keyspace, tableName));

        dropTable(keyspace, tableName);

        assertNull(fetchTableMetadata(keyspace, tableName));
    }

    @Test
    public void testDropTableWithCompositePK() throws Exception {
        String tableName = TABLE_NAME_2;
        String keyspace = getKeyspaceFromProperties();
        CreateTableInput basicCreateTableInput = getBasicCreateTableInput(getCompositePrimaryKey(), keyspace, tableName);
        getCassandraService().createTable(basicCreateTableInput);
        assertNotNull(fetchTableMetadata(keyspace, tableName));

        dropTable(keyspace, tableName);

        assertNull(fetchTableMetadata(keyspace, tableName));
    }

    void dropTable(String keyspaceName, String tableName) throws Exception {
        flowRunner("dropTable-flow")
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }
}

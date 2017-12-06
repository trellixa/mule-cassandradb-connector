/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;

import com.datastax.driver.core.TableMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.tck.junit4.matcher.ErrorTypeMatcher;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.VALID_COLUMN_1;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.VALID_LIST_COLUMN;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.QUERY_VALIDATION;

public class RenameColumnTestCase extends AbstractTestCases {
    @Before
    public  void setup() throws Exception {
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getCompositePrimaryKey(), getKeyspaceFromProperties(), TABLE_NAME_1);
        getCassandraService().createTable(basicCreateTableInput);
    }

    @After
    public void tearDown() {
        getCassandraService().dropTable(TABLE_NAME_1, getKeyspaceFromProperties());
    }

    @Test
    public void shouldRenamePKColumnWithSuccess() throws Exception {
        String renamed = "renamed";
        assertTrue(renameColumn(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_COLUMN_1, renamed));

        Thread.sleep(SLEEP_DURATION);
        TableMetadata tableMetadata = fetchTableMetadata(getKeyspaceFromProperties(), TABLE_NAME_1);
        ColumnMetadata column = tableMetadata.getColumn(renamed);
        assertNotNull(column);
    }

    @Test
    public void shouldNotRenameNonPKColumn() throws Exception {
        getCassandraService().addNewColumn(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_LIST_COLUMN, DataType.list(DataType.text()));
        renameColumnExpException(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_LIST_COLUMN, "renamed", QUERY_VALIDATION);
    }

    boolean renameColumn(String tableName, String keyspaceName, String column, String newColumn) throws Exception {
        return (boolean) flowRunner("renameColumn-flow")
                .withPayload(column)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .withVariable("newColumnName", newColumn)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    void renameColumnExpException(String tableName, String keyspaceName, String column, String newColumn, CassandraError error) throws Exception {
        flowRunner("renameColumn-flow")
                .withPayload(column)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .withVariable("newColumnName", newColumn)
                .runExpectingException(ErrorTypeMatcher.errorType(error));
    }
}

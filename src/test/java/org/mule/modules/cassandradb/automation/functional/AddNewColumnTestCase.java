/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import org.junit.*;
import org.junit.experimental.theories.Theories;
import org.junit.runner.notification.RunListener;
import org.mule.modules.cassandradb.api.AlterColumnInput;
import org.mule.modules.cassandradb.api.ColumnType;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.tck.junit4.matcher.ErrorTypeMatcher;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mule.modules.cassandradb.api.ColumnType.*;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getAlterColumnInput;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getColumns;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.VALID_COLUMN_1;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.QUERY_VALIDATION;


public class AddNewColumnTestCase extends AbstractTestCases {

    @Before
    public void setup() {
        CreateTableInput basicCreateTableInput = getBasicCreateTableInput(getColumns(), getKeyspaceFromProperties(), TABLE_NAME_1);
        getCassandraService().createTable(basicCreateTableInput);
    }

    @After
    public void tearDown() {
        getCassandraService().dropTable(TABLE_NAME_1, getKeyspaceFromProperties());
    }

    @Test
    public void testAddNewColumnOfPrimitiveTypeWithSuccess() throws Exception {
        AlterColumnInput alterColumnInput = getAlterColumnInput(DataType.text().toString() + System.currentTimeMillis(), TEXT);
        assertTrue(addNewColumn(TABLE_NAME_1, getKeyspaceFromProperties(), alterColumnInput));

        Thread.sleep(SLEEP_DURATION);
        TableMetadata tableMetadata = fetchTableMetadata(getKeyspaceFromProperties(), TABLE_NAME_1);
        ColumnMetadata column = tableMetadata.getColumn(alterColumnInput.getColumn());
        assertNotNull(column);
    }

    @Test
    public void testAddNewColumnWithSameName() throws Exception {
        AlterColumnInput alterColumnInput = getAlterColumnInput(VALID_COLUMN_1, TEXT);
        addNewColumnExpException(TABLE_NAME_1, getKeyspaceFromProperties(), alterColumnInput, QUERY_VALIDATION);
    }

    boolean addNewColumn(String tableName, String keyspaceName, AlterColumnInput alterColumnInput) throws Exception {
        return (boolean) flowRunner("addColumn-flow")
                .withPayload(alterColumnInput)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    void addNewColumnExpException(String tableName, String keyspaceName, AlterColumnInput alterColumnInput, CassandraError error) throws Exception {
        flowRunner("addColumn-flow")
                .withPayload(alterColumnInput)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .runExpectingException(ErrorTypeMatcher.errorType(error));
    }
}

/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.DataType;
import org.junit.*;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.tck.junit4.matcher.ErrorTypeMatcher;

import java.util.Map;

import static org.mule.modules.cassandradb.automation.util.TestsConstants.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.VALID_LIST_COLUMN;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.VALID_MAP_COLUMN;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.VALID_SET_COLUMN;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.QUERY_VALIDATION;

public class InsertTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), getKeyspaceFromProperties(), TABLE_NAME_1);
        getCassandraService().createTable(basicCreateTableInput);
    }

    @After
    public void tearDown()  {
        getCassandraService().dropTable(TABLE_NAME_1, getKeyspaceFromProperties());
    }

    @Test
    public void testInsertWithSuccess() throws Exception {
        insert(TABLE_NAME_1, null, TestDataBuilder.getValidEntity());
        //TODO: add validation that the objects are created.
    }

    @Test
    public void testInsertInListColumnWithSuccess() throws Exception {
        getCassandraService().addNewColumn(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_LIST_COLUMN, DataType.list(DataType.text()));

        insert(TABLE_NAME_1, null, TestDataBuilder.getValidEntityWithList());
    }

    @Test
    public void testInsertInMapColumnWithSuccess() throws Exception {
        getCassandraService().addNewColumn(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_MAP_COLUMN, DataType.map(DataType.text(), DataType.text()));

        insert(TABLE_NAME_1, null, TestDataBuilder.getValidEntityWithMap());
    }

    @Test
    public void testInsertISetColumnWithSuccess() throws Exception {
        getCassandraService().addNewColumn(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_SET_COLUMN, DataType.set(DataType.text()));

        insert(TABLE_NAME_1, null, TestDataBuilder.getValidEntityWithSet());
    }

    @Test
    public void testInsertWithInvalidInput() throws Exception {
        insertExpError(TABLE_NAME_1, null, TestDataBuilder.getInvalidEntity(), QUERY_VALIDATION);
    }

    void insert(String tableName, String keyspaceName, Map<String, Object> entity) throws Exception {
        flowRunner("insert-flow")
                .withPayload(entity)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    void insertExpError(String tableName, String keyspaceName, Map<String, Object> entity, CassandraError error) throws Exception {
        flowRunner("insert-flow")
                .withPayload(entity)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .runExpectingException(ErrorTypeMatcher.errorType(error));
    }
}

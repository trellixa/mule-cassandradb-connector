/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.DataType;
import org.junit.*;
import org.mule.modules.cassandradb.api.AlterColumnInput;
import org.mule.modules.cassandradb.api.ColumnType;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.tck.junit4.matcher.ErrorTypeMatcher;

import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getAlterColumnInput;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getPrimaryKey;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.QUERY_VALIDATION;

//@Ignore
//Test-class marked as ignored since altering of column types was removed from Cass Database v3.0.11 +
//https://issues.apache.org/jira/browse/CASSANDRA-12443

//TODO: Ignoring some cases. Review if has to expect exception
public class ChangeColumnTypeTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        getCassandraService().createTable(TestDataBuilder.getBasicCreateTableInput(getPrimaryKey(), getKeyspaceFromProperties(), TestsConstants.TABLE_NAME_1));
    }

    @After
    public void tearDown()  {
        getCassandraService().dropTable(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties());
    }

    @Test
    public void testChangeTypeFromBlobToText() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.text().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test
    public void testChangeTypeFromBlobToAscii() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.ascii().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.ASCII));
    }

    @Test
    public void testChangeTypeFromBlobToBigint() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.bigint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.BIGINT));
    }

    @Test
    public void testChangeTypeFromBlobToInt() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.cint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.INT));
    }

    @Test
    public void testChangeTypeFromBlobToTimestamp() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.timestamp().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test
    public void testChangeTypeFromBlobToTimeuuid() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.timeuuid().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.TIMEUUID));
    }

    @Test
    public void testChangeTypeFromBlobToVarchar() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.varchar().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.VARCHAR));
    }

    @Test
    public void testChangeTypeFromTextToInt() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.cint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.text());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.INT));
    }

    @Test
    public void testChangeTypeFromTextToTimestamp() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.timestamp().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.text());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test
    public void testChangeTypeFromTextToTimeuuid() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.timeuuid().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.text());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.TIMEUUID));
    }

    @Test
    public void testChangeTypeFromTextToBigint() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.bigint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.text());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.BIGINT));
    }

    @Test
    public void testChangeTypeFromTextToVarint() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.varint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.text());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test
    public void testChangeTypeFromTextToUuid() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.uuid().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.text());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.UUID));
    }

    @Test
    public void testChangeTypeFromVarcharToInt() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.cint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.varchar());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.INT));
    }

    @Test
    public void testChangeTypeFromVarcharToTimestamp() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.timestamp().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.varchar());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test
    public void testChangeTypeFromVarcharToAscii() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.ascii().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.varchar());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.ASCII));
    }

    @Test
    public void testChangeTypeFromTimestampToAscii() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.ascii().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.timestamp());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.ASCII));
    }

    @Test
    public void testChangeTypeFromTimestampToText() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.text().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.timestamp());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test
    public void testChangeTypeFromTimestampToInt() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.cint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), columnName, DataType.timestamp());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), getAlterColumnInput(columnName, ColumnType.INT));
    }

    boolean changeColumnType(String tableName, String keyspaceName, AlterColumnInput input) throws Exception {
        return (boolean) flowRunner("changeColumnType-flow")
                .withPayload(input)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    void changeColumnTypeExpException(String tableName, String keyspaceName, AlterColumnInput input) throws Exception {
        flowRunner("changeColumnType-flow")
                .withPayload(input)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .runExpectingException(ErrorTypeMatcher.errorType(QUERY_VALIDATION));
    }
}

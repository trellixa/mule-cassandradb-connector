/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.DataType;
import org.junit.*;
import org.mule.modules.cassandradb.api.ColumnType;
import org.mule.modules.cassandradb.automation.util.TestsConstants;

@Ignore
//Test-class marked as ignored since altering of column types was removed from Cass Database v3.0.11 +
//https://issues.apache.org/jira/browse/CASSANDRA-12443

//TODO: Ignoring some cases. Review if has to expect exception
public class ChangeColumnTypeTestCases extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        getCassandraService().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getPrimaryKey(), getCassandraProperties().getKeyspace(), TestsConstants.TABLE_NAME_1));
    }

    @After
    public void tearDown()  {
        getCassandraService().dropTable(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace());
    }

    @Test
    @Ignore
    public void testChangeTypeFromAsciiToBlobWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.ascii().toString() + DataType.blob().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.text());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    @Ignore
    public void testChangeTypeFromAsciiToTextWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.ascii().toString() + DataType.text().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.ascii());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test
    @Ignore
    public void testChangeTypeFromAsciiToVarcharWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.ascii().toString() + DataType.varchar().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.ascii());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARCHAR));
    }

    @Test
    @Ignore
    public void testChangeTypeFromBigintToBlobWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.bigint().toString() + DataType.blob().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.bigint());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    @Ignore
    public void testChangeTypeFromBigintToTimestampWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.bigint().toString() + DataType.timestamp().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.bigint());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test
    @Ignore
    public void testChangeTypeFromBigintToVarintWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.bigint().toString() + DataType.varint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.bigint());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test
    @Ignore
    public void testChangeTypeFromIntToBlobWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.cint().toString() + DataType.blob().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.cint());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    @Ignore
    public void testChangeTypeFromIntToVarintWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.cint().toString() + DataType.varint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.cint());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test
    @Ignore
    public void testChangeTypeFromTextToBlobWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.blob().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.text());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    @Ignore
    public void testChangeTypeFromTextToVarcharWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.varchar().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.text());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARCHAR));
    }

    @Test
    @Ignore
    public void testChangeTypeFromTimestampToBigintWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.bigint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.timestamp());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BIGINT));
    }

    @Test
    @Ignore
    public void testChangeTypeFromTimestampToBlobWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.blob().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.timestamp());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    @Ignore
    public void testChangeTypeFromTimestampToVarintWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.varint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.timestamp());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test
    @Ignore
      public void testChangeTypeFromTimeuuidToBlobWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.timeuuid().toString() + DataType.blob().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.timeuuid());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    @Ignore
    public void testChangeTypeFromTimeuuidToUuidWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.timeuuid().toString() + DataType.uuid().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.timeuuid());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.UUID));
    }

    @Test
    @Ignore
    public void testChangeTypeFromVarcharToBlobWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.blob().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.varchar());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    @Ignore
    public void testChangeTypeFromVarcharToTextWithSuccess() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.text().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.varchar());
        changeColumnType(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test
    public void testChangeTypeFromBlobToText() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.text().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test
    public void testChangeTypeFromBlobToAscii() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.ascii().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.ASCII));
    }

    @Test
    public void testChangeTypeFromBlobToBigint() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.bigint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BIGINT));
    }

    @Test
    public void testChangeTypeFromBlobToInt() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.cint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.INT));
    }

    @Test
    public void testChangeTypeFromBlobToTimestamp() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.timestamp().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test
    public void testChangeTypeFromBlobToTimeuuid() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.timeuuid().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMEUUID));
    }

    @Test
    public void testChangeTypeFromBlobToVarchar() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.varchar().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.blob());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARCHAR));
    }

    @Test
    public void testChangeTypeFromTextToInt() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.cint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.text());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.INT));
    }

    @Test
    public void testChangeTypeFromTextToTimestamp() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.timestamp().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.text());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test
    public void testChangeTypeFromTextToTimeuuid() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.timeuuid().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.text());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMEUUID));
    }

    @Test
    public void testChangeTypeFromTextToBigint() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.bigint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.text());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BIGINT));
    }

    @Test
    public void testChangeTypeFromTextToVarint() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.varint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.text());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test
    public void testChangeTypeFromTextToUuid() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.uuid().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.text());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.UUID));
    }

    @Test
    public void testChangeTypeFromVarcharToInt() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.cint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.varchar());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.INT));
    }

    @Test
    public void testChangeTypeFromVarcharToTimestamp() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.timestamp().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.varchar());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test
    public void testChangeTypeFromVarcharToAscii() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.ascii().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.varchar());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.ASCII));
    }

    @Test
    public void testChangeTypeFromTimestampToAscii() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.ascii().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.timestamp());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.ASCII));
    }

    @Test
    public void testChangeTypeFromTimestampToText() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.text().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.timestamp());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test
    public void testChangeTypeFromTimestampToInt() throws Exception {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.cint().toString();
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), columnName, DataType.timestamp());
        changeColumnTypeExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestDataBuilder.getAlterColumnInput(columnName, ColumnType.INT));
    }
}

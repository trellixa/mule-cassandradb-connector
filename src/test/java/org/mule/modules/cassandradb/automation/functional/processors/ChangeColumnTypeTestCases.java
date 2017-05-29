/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional.processors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import org.junit.Ignore;
import org.mule.modules.cassandradb.automation.functional.TestDataBuilder;
import org.mule.modules.cassandradb.metadata.ColumnType;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.utils.CassandraDBException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@Ignore
//Test-class marked as ignored since altering of column types was removed from Cass Database v3.0.11 +
//https://issues.apache.org/jira/browse/CASSANDRA-12443
public class ChangeColumnTypeTestCases extends CassandraAbstractTestCases {

    @BeforeClass
    public static void setup() throws Exception {
        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getPrimaryKey(), cassConfig.getKeyspace(), TestsConstants.TABLE_NAME_1));
    }

    @AfterClass
    public static void tearDown()  {
        cassClient.dropTable(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace());
    }

    @Test
    public void testChangeTypeFromAsciiToBlobWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.ascii().toString() + DataType.blob().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromAsciiToTextWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.ascii().toString() + DataType.text().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.ascii());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test
    public void testChangeTypeFromAsciiToVarcharWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.ascii().toString() + DataType.varchar().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.ascii());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARCHAR));
    }

    @Test
    public void testChangeTypeFromBigintToBlobWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.bigint().toString() + DataType.blob().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.bigint());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromBigintToTimestampWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.bigint().toString() + DataType.timestamp().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.bigint());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test
    public void testChangeTypeFromBigintToVarintWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.bigint().toString() + DataType.varint().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.bigint());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test
    public void testChangeTypeFromIntToBlobWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.cint().toString() + DataType.blob().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.cint());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromIntToVarintWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.cint().toString() + DataType.varint().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.cint());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test
    public void testChangeTypeFromTextToBlobWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.blob().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromTextToVarcharWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.varchar().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARCHAR));
    }

    @Test
    public void testChangeTypeFromTimestampToBigintWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.bigint().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.timestamp());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BIGINT));
    }

    @Test
    public void testChangeTypeFromTimestampToBlobWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.blob().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.timestamp());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromTimestampToVarintWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.varint().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.timestamp());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test
      public void testChangeTypeFromTimeuuidToBlobWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.timeuuid().toString() + DataType.blob().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.timeuuid());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromTimeuuidToUuidWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.timeuuid().toString() + DataType.uuid().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.timeuuid());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.UUID));
    }

    @Test
    public void testChangeTypeFromVarcharToBlobWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.blob().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.varchar());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromVarcharToTextWithSuccess() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.text().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.varchar());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToText() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.text().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToAscii() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.ascii().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.ASCII));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToBigint() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.bigint().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BIGINT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToInt() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.cint().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.INT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToTimestamp() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.timestamp().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToTimeuuid() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.timeuuid().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMEUUID));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToVarchar() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.blob().toString() + DataType.varchar().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARCHAR));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTextToInt() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.cint().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.INT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTextToTimestamp() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.timestamp().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTextToTimeuuid() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.timeuuid().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMEUUID));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTextToBigint() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.bigint().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BIGINT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTextToVarint() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.varint().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTextToUuid() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.text().toString() + DataType.uuid().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.UUID));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromVarcharToInt() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.cint().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.varchar());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.INT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromVarcharToTimestamp() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.timestamp().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.varchar());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromVarcharToAscii() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.varchar().toString() + DataType.ascii().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.varchar());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.ASCII));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTimestampToAscii() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.ascii().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.timestamp());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.ASCII));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTimestampToText() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.text().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.timestamp());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTimestampToInt() throws CassandraDBException {
        String columnName = TestsConstants.COLUMN + DataType.timestamp().toString() + DataType.cint().toString();
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), columnName, DataType.timestamp());
        getConnector().changeColumnType(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.INT));
    }
}

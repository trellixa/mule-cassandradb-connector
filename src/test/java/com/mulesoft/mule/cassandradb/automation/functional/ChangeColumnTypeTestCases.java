/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.metadata.ColumnType;
import com.mulesoft.mule.cassandradb.util.ConstantsTest;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ChangeColumnTypeTestCases extends CassandraDBConnectorAbstractTestCase {

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;

    @BeforeClass
    public static void setup() throws Exception {
        cassConfig = getClientConfig();
        cassClient = configureClient(cassConfig);
        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME, TestDataBuilder.getValidEntity());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        cassClient.dropTable(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace());
    }

    @Test
    public void testChangeTypeFromAsciiToBlobWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.ascii().toString() + DataType.blob().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.ascii());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromAsciiToTextWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.ascii().toString() + DataType.text().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.ascii());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test
    public void testChangeTypeFromAsciiToVarcharWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.ascii().toString() + DataType.varchar().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.ascii());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARCHAR));
    }

    @Test
    public void testChangeTypeFromBigintToBlobWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.bigint().toString() + DataType.blob().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.bigint());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromBigintToTimestampWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.bigint().toString() + DataType.timestamp().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.bigint());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test
    public void testChangeTypeFromBigintToVarintWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.bigint().toString() + DataType.varint().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.bigint());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test
    public void testChangeTypeFromIntToBlobWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.cint().toString() + DataType.blob().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.cint());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromIntToVarintWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.cint().toString() + DataType.varint().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.cint());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test
    public void testChangeTypeFromTextToBlobWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.text().toString() + DataType.blob().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromTextToVarcharWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.text().toString() + DataType.varchar().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARCHAR));
    }

    @Test
    public void testChangeTypeFromTimestampToBigintWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.timestamp().toString() + DataType.bigint().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.timestamp());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BIGINT));
    }

    @Test
    public void testChangeTypeFromTimestampToBlobWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.timestamp().toString() + DataType.blob().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.timestamp());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromTimestampToVarintWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.timestamp().toString() + DataType.varint().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.timestamp());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test
      public void testChangeTypeFromTimeuuidToBlobWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.timeuuid().toString() + DataType.blob().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.timeuuid());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromTimeuuidToUuidWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.timeuuid().toString() + DataType.uuid().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.timeuuid());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.UUID));
    }

    @Test
    public void testChangeTypeFromVarcharToBlobWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.varchar().toString() + DataType.blob().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.varchar());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BLOB));
    }

    @Test
    public void testChangeTypeFromVarcharToTextWithSuccess() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.varchar().toString() + DataType.text().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.varchar());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToText() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.blob().toString() + DataType.text().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToAscii() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.blob().toString() + DataType.ascii().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.ASCII));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToBigint() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.blob().toString() + DataType.bigint().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BIGINT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToInt() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.blob().toString() + DataType.cint().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.INT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToTimestamp() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.blob().toString() + DataType.timestamp().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToTimeuuid() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.blob().toString() + DataType.timeuuid().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMEUUID));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromBlobToVarchar() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.blob().toString() + DataType.varchar().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.blob());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARCHAR));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTextToInt() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.text().toString() + DataType.cint().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.INT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTextToTimestamp() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.text().toString() + DataType.timestamp().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTextToTimeuuid() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.text().toString() + DataType.timeuuid().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMEUUID));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTextToBigint() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.text().toString() + DataType.bigint().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.BIGINT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTextToVarint() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.text().toString() + DataType.varint().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.VARINT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTextToUuid() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.text().toString() + DataType.uuid().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.text());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.UUID));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromVarcharToInt() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.varchar().toString() + DataType.cint().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.varchar());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.INT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromVarcharToTimestamp() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.varchar().toString() + DataType.timestamp().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.varchar());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TIMESTAMP));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromVarcharToAscii() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.varchar().toString() + DataType.ascii().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.varchar());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.ASCII));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTimestampToAscii() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.timestamp().toString() + DataType.ascii().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.timestamp());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.ASCII));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTimestampToText() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.timestamp().toString() + DataType.text().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.timestamp());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.TEXT));
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void testChangeTypeFromTimestampToInt() throws CassandraDBException {
        String columnName = ConstantsTest.COLUMN + DataType.timestamp().toString() + DataType.cint().toString();
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace(), columnName, DataType.timestamp());
        getConnector().changeColumnType(ConstantsTest.TABLE_NAME, TestDataBuilder.getAlterColumnInput(columnName, ColumnType.INT));
    }
}

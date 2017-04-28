/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional;

import com.datastax.driver.core.DataType;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.util.ConstantsTest;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AddNewColumnTestCases extends CassandraDBConnectorAbstractTestCase {

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;
    Object text = DataType.text().toString();

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
    public void testAddNewColumnOfPrimitiveTypeWithSuccess() throws CassandraDBException {
        getConnector().addNewColumn(ConstantsTest.TABLE_NAME,
                TestDataBuilder.getAddNewColumnInput(DataType.text().toString() + System.currentTimeMillis(), text));
    }

    @Test
    public void testAddNewColumnOfListTypeWithSuccess() throws CassandraDBException {
        getConnector().addNewColumn(ConstantsTest.TABLE_NAME,
                TestDataBuilder.getAddNewColumnInput(DataType.list(DataType.text()).toString() + System.currentTimeMillis(),
                        TestDataBuilder.getCollectionType(ConstantsTest.LIST, DataType.text().toString())));
    }

    @Test
    public void testAddNewColumnOfMapTypeWithSuccess() throws CassandraDBException {
        getConnector().addNewColumn(ConstantsTest.TABLE_NAME,
                TestDataBuilder.getAddNewColumnInput(DataType.map(DataType.text(), DataType.text()).toString() + System.currentTimeMillis(),
                        TestDataBuilder.getCollectionType(ConstantsTest.MAP, TestDataBuilder.getCollectionType(DataType.text().toString(), DataType.text().toString()))));
    }

    @Test
    public void testAddNewColumnOfSetTypeWithSuccess() throws CassandraDBException {
        getConnector().addNewColumn(ConstantsTest.TABLE_NAME,
                TestDataBuilder.getAddNewColumnInput(DataType.set(DataType.text()).toString() + System.currentTimeMillis(),
                        TestDataBuilder.getCollectionType(ConstantsTest.SET, DataType.text().toString())));
    }

    @Test(expected = CassandraDBException.class)
    public void testAddNewColumnWithSameName() throws CassandraDBException {
        getConnector().addNewColumn(ConstantsTest.TABLE_NAME, TestDataBuilder.getAddNewColumnInput(ConstantsTest.VALID_COLUMN, text));
    }

}

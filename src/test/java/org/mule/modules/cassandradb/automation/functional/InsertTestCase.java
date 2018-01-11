///**
// * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package org.mule.modules.cassandradb.automation.functional;
//
//import com.datastax.driver.core.DataType;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.mule.modules.cassandradb.api.CreateTableInput;
//
//import java.util.List;
//import java.util.Map;
//
//import static java.lang.String.format;
//import static org.junit.Assert.assertEquals;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_1;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.VALID_LIST_COLUMN;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.VALID_MAP_COLUMN;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.VALID_SET_COLUMN;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getColumns;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getInvalidEntity;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidEntityWithList;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidEntityWithMap;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidEntityWithSet;
//import static org.mule.modules.cassandradb.internal.exception.CassandraError.QUERY_VALIDATION;
//import static org.mule.tck.junit4.matcher.ErrorTypeMatcher.errorType;
//
//public class InsertTestCase extends AbstractTestCases {
//
//    @Before
//    public void setup() throws Exception {
//        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(getColumns(), getKeyspaceFromProperties(), TABLE_NAME_1);
//        getCassandraService().createTable(basicCreateTableInput);
//    }
//
//    @After
//    public void tearDown()  {
//        getCassandraService().dropTable(TABLE_NAME_1, getKeyspaceFromProperties());
//    }
//
//    @Test
//    public void testInsertWithSuccess() throws Exception {
//        insert(TABLE_NAME_1, null, TestDataBuilder.getValidEntity());
//
//        Thread.sleep(SLEEP_DURATION);
//        String query = format("SELECT * FROM %s.%s", getKeyspaceFromProperties(), TABLE_NAME_1);
//        List<Map<String, Object>> select = getCassandraService().select(query, null);
//        assertEquals(1, select.size());
//    }
//
//    @Test
//    public void testInsertInListColumnWithSuccess() throws Exception {
//        getCassandraService().addNewColumn(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_LIST_COLUMN, DataType.list(DataType.text()));
//
//        insert(TABLE_NAME_1, null, getValidEntityWithList());
//    }
//
//    @Test
//    public void testInsertInMapColumnWithSuccess() throws Exception {
//        getCassandraService().addNewColumn(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_MAP_COLUMN, DataType.map(DataType.text(), DataType.text()));
//
//        insert(TABLE_NAME_1, null, getValidEntityWithMap());
//    }
//
//    @Test
//    public void testInsertISetColumnWithSuccess() throws Exception {
//        getCassandraService().addNewColumn(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_SET_COLUMN, DataType.set(DataType.text()));
//
//        insert(TABLE_NAME_1, null, getValidEntityWithSet());
//    }
//
//    @Test
//    public void testInsertWithInvalidInput() throws Exception {
//        insertExpError(TABLE_NAME_1, null, getInvalidEntity(), QUERY_VALIDATION);
//    }
//
//
//}

/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional.processors;

import com.datastax.driver.core.DataType;
import org.mule.modules.cassandradb.automation.functional.TestDataBuilder;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.utils.CassandraDBException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeleteRowsTestCases extends CassandraAbstractTestCases {

    @BeforeClass
    public static void setup() throws Exception {
        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), cassConfig.getKeyspace(), TestsConstants.TABLE_NAME_1));
    }

    @AfterClass
    public static void tearDown() {
        cassClient.dropTable(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace());
    }

    @Test
    public void testDeleteRowUsingEqWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.insert(cassConfig.getKeyspace(), TestsConstants.TABLE_NAME_1, TestDataBuilder.getValidEntity());

        getConnector().deleteRows(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getPayloadColumnsAndFilters(null, TestDataBuilder.getValidWhereClauseWithEq()));
    }


    @Test
    public void testDeleteRowUsingINWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.insert(cassConfig.getKeyspace(), TestsConstants.TABLE_NAME_1, TestDataBuilder.getValidEntity());

        getConnector().deleteRows(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getPayloadColumnsAndFilters(null, TestDataBuilder.getValidWhereClauseWithIN()));
    }

}

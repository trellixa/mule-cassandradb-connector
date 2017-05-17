/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional.processors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.mule.modules.cassandradb.automation.functional.TestDataBuilder;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.utils.CassandraDBException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RenameColumnTestCases extends CassandraAbstractTestCases {
    @BeforeClass
    public static void setup() throws Exception {
        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getCompositePrimaryKey(), cassConfig.getKeyspace(), TestsConstants.TABLE_NAME_1));
    }

    @AfterClass
    public static void tearDown() {
        cassClient.dropTable(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace());
    }

    @Test
    public void shouldRenamePKColumnWithSuccess() throws CassandraDBException {
        getConnector().renameColumn(TestsConstants.TABLE_NAME_1, null, TestsConstants.VALID_COLUMN_1, "renamed");
    }

    @Test(expected = InvalidQueryException.class)
    public void shouldNotRenameNonPKColumn() throws CassandraDBException {
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), TestsConstants.VALID_LIST_COLUMN, DataType.list(DataType.text()));
        getConnector().renameColumn(TestsConstants.TABLE_NAME_1, null, TestsConstants.VALID_LIST_COLUMN, "renamed");
    }
}

/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.automation.util.TestsConstants;

import static org.junit.Assert.assertTrue;

public class RenameColumnTestCase extends AbstractTestCases {
    @Before
    public  void setup() throws Exception {
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getCompositePrimaryKey(), getKeyspaceFromProperties(), TestsConstants.TABLE_NAME_1);
        getCassandraService().createTable(basicCreateTableInput);
    }

    @After
    public void tearDown() {
        getCassandraService().dropTable(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties());
    }

    @Test
    public void shouldRenamePKColumnWithSuccess() throws Exception {
        assertTrue(renameColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), TestsConstants.VALID_COLUMN_1, "renamed"));
    }

    @Test
    public void shouldNotRenameNonPKColumn() throws Exception {
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), TestsConstants.VALID_LIST_COLUMN, DataType.list(DataType.text()));
        renameColumnExpException(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), TestsConstants.VALID_LIST_COLUMN, "renamed");
    }
}

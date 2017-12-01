/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.automation.util.TestsConstants;

import static org.junit.Assert.assertTrue;


public class DropColumnTestCases extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), getCassandraProperties().getKeyspace(), TestsConstants.TABLE_NAME_1);
        getCassandraService().createTable(basicCreateTableInput);
    }

    @After
    public void tearDown() {
        getCassandraService().dropTable(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace());
    }

    @Test
    public void testRemoveColumnWithSuccess() throws Exception {
        assertTrue(dropColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestsConstants.VALID_COLUMN_1));
        assertTrue(dropColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestsConstants.VALID_COLUMN_2));
    }


    @Test
    public void testRemoveColumnWithInvalidName() throws Exception {
        dropColumnExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestsConstants.COLUMN);
    }
}

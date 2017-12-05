/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.*;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.automation.util.TestsConstants;

public class UpdateTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), getKeyspaceFromProperties(), TestsConstants.TABLE_NAME_1);
        getCassandraService().createTable(basicCreateTableInput);
        getCassandraService().insert(getKeyspaceFromProperties(), TestsConstants.TABLE_NAME_1, TestDataBuilder.getValidEntity());
    }

    @After
    public void tearDown() {
        getCassandraService().dropTable(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties());
    }

    @Test
    public void testUpdateUsingEqWithSuccess() throws Exception {
        update(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidEntityForUpdate(), TestDataBuilder.getValidWhereClauseWithEq()));
    }


    @Test
    public void testUpdateUsingInWithSuccess() throws Exception {
        update(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidEntityForUpdate(),
                TestDataBuilder.getValidWhereClauseWithIN()));
    }

    @Test
    public void testUpdateWithInvalidInput() throws Exception {
        updateExpException(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getInvalidEntity(), TestDataBuilder.getValidWhereClauseWithEq()));
    }

    @Test
    public void testUpdateWithInvalidWhereClause() throws Exception {
        updateExpException(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getInvalidEntity(), TestDataBuilder.getInvalidWhereClause()));
    }
}

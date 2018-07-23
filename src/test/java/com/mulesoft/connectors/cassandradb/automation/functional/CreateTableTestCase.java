/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.connectors.cassandradb.automation.functional;


import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.fail;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_1;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_2;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getColumns;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getCompositePrimaryKey;

public class CreateTableTestCase extends AbstractTestCases {
    @After
    public void tearDown() throws Exception {
        dropTable(TABLE_NAME_1, testKeyspace);
        dropTable(TABLE_NAME_2, testKeyspace);
    }

    @Test
    public void testCreateTableWithSuccess() {
        try{
            createTable(getBasicCreateTableInput(getColumns(), testKeyspace, TABLE_NAME_1));
        } catch (Exception e){
            fail();
        }
    }

    @Test
    public void testCreateTableWithCompositePKWithSuccess() {
        try{
            createTable(getBasicCreateTableInput(getCompositePrimaryKey(), testKeyspace, TABLE_NAME_2));
        } catch (Exception e){
            fail();
        }
    }
}

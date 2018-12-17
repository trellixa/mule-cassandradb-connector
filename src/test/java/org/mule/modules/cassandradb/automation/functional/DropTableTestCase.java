/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.Ignore;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateTableInput;

import static org.junit.Assert.fail;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_2;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getCompositePrimaryKey;
@Ignore

public class DropTableTestCase extends AbstractTestCases {

    @Test
    public void testDropTable() throws Exception {
        try{
            createTable(getBasicCreateTableInput(TestDataBuilder.getColumns(), testKeyspace, TABLE_NAME_1));
            dropTable(testKeyspace, TABLE_NAME_1);
        } catch (Exception e){
            fail();
        }
    }

    @Test
    public void testDropTableWithCompositePK() throws Exception {
        try{
            CreateTableInput basicCreateTableInput = getBasicCreateTableInput(getCompositePrimaryKey(), testKeyspace, TABLE_NAME_2);
            createTable(basicCreateTableInput);
            dropTable(testKeyspace, TABLE_NAME_2);
        } catch (Exception e){
            fail();
        }
    }
}

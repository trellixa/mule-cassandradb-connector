/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.mule.modules.cassandradb.api.ColumnType;
import org.mule.test.runner.RunnerDelegateTo;

import java.util.List;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getAlterColumnInput;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getPrimaryKey;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getRandomColumnName;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getTestTypes;

@Ignore("The 'alter table' operation is not allowed since Cassandra v3.0.10")
@RunnerDelegateTo(Parameterized.class)
public class ChangeColumnTypeTestCase extends AbstractTestCases {


    @Before
    public void setup() throws Exception {
        createTable(getBasicCreateTableInput(getPrimaryKey(), testKeyspace, TABLE_NAME_1));
    }

    @After
    public void tearDown() throws Exception {
        dropTable(TABLE_NAME_1, testKeyspace);
    }

    @Parameterized.Parameters(name = "{0} to {1}")
    public static List<ColumnType[]> data() {
        return getTestTypes();
    }

    private ColumnType initialType;
    private ColumnType typeToChange;

    public ChangeColumnTypeTestCase(ColumnType initialType, ColumnType typeToChange){
        this.initialType = initialType;
        this.typeToChange = typeToChange;
    }

    @Test
    public void testChangeTypeToAnIncompatibleType() throws Exception {
        try{
            String columnName = getRandomColumnName();
            addNewColumn(TABLE_NAME_1, testKeyspace, getAlterColumnInput(columnName, initialType));
            changeColumnType(TABLE_NAME_1, testKeyspace, getAlterColumnInput(columnName, typeToChange));
        } catch(Exception e){
            assertThat(e.getMessage(), endsWith("types are incompatible."));
        }
    }
}

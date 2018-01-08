/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;


import com.datastax.driver.core.TableMetadata;
import org.junit.After;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateTableInput;

import static org.junit.Assert.assertNotNull;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.TABLE_NAME_2;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getBasicCreateTableInput;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getColumns;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getCompositePrimaryKey;

public class CreateTableTestCase extends AbstractTestCases {

    @After
    public void tearDown() {
        getCassandraService().dropTable(TABLE_NAME_1, getKeyspaceFromProperties());
        getCassandraService().dropTable(TABLE_NAME_2, getKeyspaceFromProperties());
    }

    @Test
    public void testCreateTableWithSuccess() throws Exception {
        CreateTableInput basicCreateTableInput = getBasicCreateTableInput(getColumns(), getKeyspaceFromProperties(), TABLE_NAME_1);
        createTable(basicCreateTableInput);

        Thread.sleep(SLEEP_DURATION);
        TableMetadata tableMetadata = fetchTableMetadata(getKeyspaceFromProperties(), TABLE_NAME_1);
        assertNotNull(tableMetadata);
    }

    @Test
    public void testCreateTableWithCompositePKWithSuccess() throws Exception {
        CreateTableInput basicCreateTableInput = getBasicCreateTableInput(getCompositePrimaryKey(), getKeyspaceFromProperties(), TABLE_NAME_2);
        createTable(basicCreateTableInput);
    }

    void createTable(CreateTableInput basicCreateTableInput) throws Exception {
        flowRunner("createTable-flow")
                .withPayload(basicCreateTableInput)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }
}

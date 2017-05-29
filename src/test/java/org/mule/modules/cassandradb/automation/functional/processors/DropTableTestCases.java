/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional.processors;

import org.junit.Test;
import org.mule.modules.cassandradb.automation.functional.TestDataBuilder;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.utils.CassandraDBException;

import static org.junit.Assert.assertTrue;

public class DropTableTestCases extends CassandraAbstractTestCases {

    @Test
    public void testDropTable() throws CassandraDBException {
        getConnector().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), cassConfig.getKeyspace(), TestsConstants.TABLE_NAME_1));

        boolean dropResult = getConnector().dropTable(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace());

        assertTrue(dropResult);
    }

    @Test
    public void testDropTableWithCompositePK() throws CassandraDBException {
        getConnector().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getCompositePrimaryKey(), cassConfig.getKeyspace(), TestsConstants.TABLE_NAME_2));

        boolean dropResult = getConnector().dropTable(TestsConstants.TABLE_NAME_2, cassConfig.getKeyspace());

        assertTrue(dropResult);
    }
}

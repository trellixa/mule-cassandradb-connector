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

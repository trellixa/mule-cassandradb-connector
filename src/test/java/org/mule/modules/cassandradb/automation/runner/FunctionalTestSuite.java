/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.runner;

import org.mule.modules.cassandradb.automation.functional.metadata.CassandraCategoryMetaDataTestCases;
import org.mule.modules.cassandradb.automation.functional.metadata.CassandraOnlyWithFiltersCategoryMetaDataTestCases;
import org.mule.modules.cassandradb.automation.functional.metadata.CassandraWithFiltersCategoryMetaDataTestCases;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.mule.modules.cassandradb.CassandraDBConnector;
import org.mule.modules.cassandradb.automation.functional.processors.*;
import org.mule.tools.devkit.ctf.mockup.ConnectorTestContext;
import org.mule.tools.devkit.ctf.platform.PlatformManager;

@RunWith(Categories.class)
@SuiteClasses({
        InsertTestCases.class,
        SelectTestCases.class,
        UpdateTestCases.class,
        DeleteRowsTestCases.class,
        DeleteColumnsValueTestCases.class,
        ChangeColumnTypeTestCases.class,
        AddNewColumnTestCases.class,
        DropColumnTestCases.class,
        RenameColumnTestCases.class,
        CreateKeyspaceTestCases.class,
        DropKeyspaceTestCases.class,
        CreateTableTestCases.class,
        DropTableTestCases.class,
        GetTableNamesFromKeyspaceTestCases.class,
        CassandraCategoryMetaDataTestCases.class,
        CassandraOnlyWithFiltersCategoryMetaDataTestCases.class,
        CassandraWithFiltersCategoryMetaDataTestCases.class,
        ExecuteCQLQueryTestCases.class
})
public class FunctionalTestSuite {

    @BeforeClass
    public static void initializeSuite() {
        ConnectorTestContext.initialize(CassandraDBConnector.class);
    }

    @AfterClass
    public static void shutdownSuite() throws Exception {
        ConnectorTestContext<CassandraDBConnector> context = ConnectorTestContext.getInstance();
        PlatformManager platform = context.getPlatformManager();
        platform.shutdown();
    }

}

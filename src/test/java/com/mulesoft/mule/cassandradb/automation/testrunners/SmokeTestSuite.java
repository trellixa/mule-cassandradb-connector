/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.testrunners;

import com.mulesoft.mule.cassandradb.automation.SmokeTests;
import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.IncludeCategory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Categories.class)
@IncludeCategory(SmokeTests.class)
@SuiteClasses({
//        AddTestCases.class, BatchMutableTestCases.class,
//        DescribeClusterNameTestCases.class, DescribeKeyspaceTestCases.class,
//        ExecuteCqlQueryTestCases.class, GetCountTestCases.class, GetRowTestCases.class,
//        GetTestCases.class, InsertTestCases.class, InsertFromMapTestCases.class,
//        RemoveTestCases.class, SystemAddColumnFamilyFromObjectTestCases.class,
//        SystemAddKeyspaceWithParamsTestCases.class, SystemDropColumnFamilyTestCases.class,
//        SystemDropKeyspaceTestCases.class, TruncateTestCases.class
})
public class SmokeTestSuite {
}

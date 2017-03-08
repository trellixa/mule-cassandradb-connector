/**
 * Mule Cassandra Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.mulesoft.mule.cassandradb.automation.testrunners;

import com.mulesoft.mule.cassandradb.automation.RegressionTests;
import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.IncludeCategory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Categories.class)
@IncludeCategory(RegressionTests.class)
@SuiteClasses({
//        AddTestCases.class, BatchMutableTestCases.class,
//        DescribeClusterNameTestCases.class, DescribeKeyspacesTestCases.class,
//        DescribeKeyspaceTestCases.class, DescribePartitionerTestCases.class,
//        DescribeRingTestCases.class, DescribeSchemaVersionsTestCases.class,
//        DescribeSnitchTestCases.class, DescribeVersionTestCases.class,
//        ExecuteCqlQueryTestCases.class, GetCountTestCases.class,
//        GetIndexedSlicesTestCases.class, GetRangeSlicesTestCases.class,
//        GetRowTestCases.class, GetSliceTestCases.class,
//        GetTestCases.class, InsertFromMapTestCases.class,
//        InsertTestCases.class, MultiGetCountTestCases.class,
//        MultiGetSliceTestCases.class, RemoveCounterTestCases.class,
//        RemoveTestCases.class, SetQueryKeyspaceTestCases.class,
//        SystemAddColumnFamilyFromObjectTestCases.class,
//        SystemAddColumnFamilyFromObjectWithSimpleNamesTestCases.class,
//        SystemAddColumnFamilyWithParamsTestCases.class,
//        SystemAddKeyspaceFromObjectTestCases.class,
//        SystemAddKeyspaceWithParamsTestCases.class,
//        SystemDropColumnFamilyTestCases.class, SystemDropKeyspaceTestCases.class,
//        SystemUpdateColumnFamilyTestCases.class, SystemUpdateKeyspaceTestCases.class,
//        TruncateTestCases.class
})
public class RegressionTestSuite {
}

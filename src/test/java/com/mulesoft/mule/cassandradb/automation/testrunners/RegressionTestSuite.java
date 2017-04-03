/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
///**
// * Mule Cassandra Connector
// *
// * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
// *
// * The software in this package is published under the terms of the CPAL v1.0
// * license, a copy of which has been included with this distribution in the
// * LICENSE.txt file.
// */
//
//package com.mulesoft.mule.cassandradb.automation.testrunners;
//
//import com.mulesoft.mule.cassandradb.CassandraDBConnector;
//import com.mulesoft.mule.cassandradb.automation.RegressionTests;
//import com.mulesoft.mule.cassandradb.automation.functional.QueryMetadataIT;
//import org.junit.AfterClass;
//import org.junit.BeforeClass;
//import org.junit.experimental.categories.Categories;
//import org.junit.experimental.categories.Categories.IncludeCategory;
//import org.junit.runner.RunWith;
//import org.junit.runners.Suite.SuiteClasses;
//import org.mule.api.annotations.Connector;
//import org.mule.tools.devkit.ctf.mockup.ConnectorTestContext;
//import org.mule.tools.devkit.ctf.platform.PlatformManager;
//
//@RunWith(Categories.class)
//@IncludeCategory(RegressionTests.class)
//@SuiteClasses({
////        AddTestCases.class, BatchMutableTestCases.class,
////        DescribeClusterNameTestCases.class, DescribeKeyspacesTestCases.class,
////        DescribeKeyspaceTestCases.class, DescribePartitionerTestCases.class,
////        DescribeRingTestCases.class, DescribeSchemaVersionsTestCases.class,
////        DescribeSnitchTestCases.class, DescribeVersionTestCases.class,
////        ExecuteCqlQueryTestCases.class, GetCountTestCases.class,
////        GetIndexedSlicesTestCases.class, GetRangeSlicesTestCases.class,
////        GetRowTestCases.class, GetSliceTestCases.class,
////        GetTestCases.class, InsertFromMapTestCases.class,
////        InsertTestCases.class, MultiGetCountTestCases.class,
////        MultiGetSliceTestCases.class, RemoveCounterTestCases.class,
////        RemoveTestCases.class, SetQueryKeyspaceTestCases.class,
////        SystemAddColumnFamilyFromObjectTestCases.class,
////        SystemAddColumnFamilyFromObjectWithSimpleNamesTestCases.class,
////        SystemAddColumnFamilyWithParamsTestCases.class,
////        SystemAddKeyspaceFromObjectTestCases.class,
////        SystemAddKeyspaceWithParamsTestCases.class,
////        SystemDropColumnFamilyTestCases.class, SystemDropKeyspaceTestCases.class,
////        SystemUpdateColumnFamilyTestCases.class, SystemUpdateKeyspaceTestCases.class,
////        TruncateTestCases.class
//        QueryMetadataIT.class
//})
//public class RegressionTestSuite {
//    @BeforeClass
//    public static void initializeSuite(){
//        ConnectorTestContext.initialize(Connector.class);
//    }
//    @AfterClass
//    public static void shutdownSuite() throws Exception{
//        ConnectorTestContext<Connector> context = ConnectorTestContext.getInstance();
//        PlatformManager platform =  context.getPlatformManager();
//        platform.shutdown();
//    }
//}

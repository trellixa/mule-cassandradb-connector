/**
 * Mule Cassandra Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.mulesoft.mule.cassandradb.automation.testcases;

import com.mulesoft.mule.cassandradb.automation.RegressionTests;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.tests.ConnectorTestUtils;

import static org.junit.Assert.*;

public class SystemAddColumnFamilyWithParamsTestCases extends CassandraDBTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("systemAddColumnFamilyWithParamsTestData");

        runFlowAndGetPayload("system-add-keyspace-with-params");
    }

    @Category({RegressionTests.class})
    @Test
    public void testSystemAddColumnFamilyWithParams() {
        try {
            Object result = runFlowAndGetPayload("system-add-column-family-with-params");
            assertNotNull(result);

            KsDef keyspace = runFlowAndGetPayload("describe-keyspace");
            assertEquals(keyspace.getCf_defsSize(), 1);
            CfDef columnDef = keyspace.getCf_defs().get(0);
            assertEquals(columnDef.getName(), getTestRunMessageValue("columnFamily"));
            assertEquals(columnDef.getComparator_type(), getTestRunMessageValue("comparatorType"));
            assertEquals(columnDef.getKey_validation_class(), getTestRunMessageValue("keyValidationClass"));
        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("system-drop-keyspace");
    }
}

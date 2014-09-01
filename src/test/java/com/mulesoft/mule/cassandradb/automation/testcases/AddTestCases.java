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
import com.mulesoft.mule.cassandradb.automation.SmokeTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.tests.ConnectorTestUtils;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AddTestCases extends CassandraDBTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("addTestData");

        runFlowAndGetPayload("system-add-keyspace-from-object");
        runFlowAndGetPayload("set-query-keyspace");

    }

    @Category({RegressionTests.class, SmokeTests.class})
    @Test
    public void testAdd() {
        try {

            runFlowAndGetPayload("add");

            Map<String, Long> data = runFlowAndGetPayload("get");
            Long expectedValue = new Long((String) getTestRunMessageValue("counterValue"));
            assertEquals(data.get(getTestRunMessageValue("counterName")), expectedValue);

            runFlowAndGetPayload("add");

            data = runFlowAndGetPayload("get");
            expectedValue += new Long((String) getTestRunMessageValue("counterValue"));
            assertEquals(data.get(getTestRunMessageValue("counterName")), expectedValue);

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("system-drop-keyspace");
    }
}

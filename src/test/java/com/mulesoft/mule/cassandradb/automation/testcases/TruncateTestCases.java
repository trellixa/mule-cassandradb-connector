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
import org.apache.cassandra.thrift.NotFoundException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.tests.ConnectorTestUtils;

import java.util.Map;

import static org.junit.Assert.*;

public class TruncateTestCases extends CassandraDBTestParent {

    @Before
    public void setUp() throws Exception {

        initializeTestRunMessage("truncateTestData");

        runFlowAndGetPayload("system-add-keyspace-with-params");

        runFlowAndGetPayload("system-add-column-family-with-params");

        runFlowAndGetPayload("insert");
    }

    @Category({RegressionTests.class, SmokeTests.class})
    @Test
    public void testTruncate() {
        Map<String, String> data;
        try {

            data = runFlowAndGetPayload("get");
            assertTrue(data.size() > 0);
            assertEquals(data.get("full_name"), getTestRunMessageValue("columnValue"));

            runFlowAndGetPayload("truncate");

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }

        // Get the column family should throw an error as
        // truncate removes all data from a column family.
        try {
            runFlowAndGetPayload("get");
        } catch (Exception e) {
            if (e.getCause() instanceof NotFoundException) {
                NotFoundException nfe = (NotFoundException) e.getCause();
                assertNull(nfe.getMessage());
            } else {
                fail(ConnectorTestUtils.getStackTrace(e));
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("system-drop-keyspace");
    }

}

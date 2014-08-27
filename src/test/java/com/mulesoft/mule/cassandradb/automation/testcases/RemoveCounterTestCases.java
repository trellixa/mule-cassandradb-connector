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
import org.apache.cassandra.thrift.NotFoundException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.tests.ConnectorTestUtils;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class RemoveCounterTestCases extends CassandraDBTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("removeCounterTestData");

        runFlowAndGetPayload("system-add-keyspace-from-object");
        runFlowAndGetPayload("set-query-keyspace");
        runFlowAndGetPayload("add");
    }

    @Category({RegressionTests.class})
    @Test
    public void testRemoveCounter() {
        try {

            runFlowAndGetPayload("remove-counter");

            // Get the column family should throw an error as
            // remove-counter, clears all data from a column family.
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

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("system-drop-keyspace");
    }
}
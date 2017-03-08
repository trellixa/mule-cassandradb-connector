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
import org.apache.cassandra.thrift.KsDef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.tests.ConnectorTestUtils;

import static org.junit.Assert.*;

public class SystemAddKeyspaceFromObjectTestCases extends CassandraDBTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("systemAddKeyspaceFromObjectTestData");
    }

    @Category({RegressionTests.class})
    @Test
    public void testSystemAddKeyspaceFromObject() {
        try {
            Object result = runFlowAndGetPayload("system-add-keyspace-from-object");
            assertNotNull(result);
            KsDef keyspace = runFlowAndGetPayload("describe-keyspace");
            assertEquals(keyspace.getName(), getTestRunMessageValue("keyspace"));
        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("system-drop-keyspace");
    }
}
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
import org.apache.cassandra.thrift.KsDef;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.tests.ConnectorTestUtils;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SystemDropKeyspaceTestCases extends CassandraDBTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("systemDropKeyspaceTestData");
    }

    @Category({RegressionTests.class, SmokeTests.class})
    @Test
    public void testSystemDropKeyspace() {
        try {
            runFlowAndGetPayload("system-add-keyspace-from-object");

            List<KsDef> keyspaces = runFlowAndGetPayload("describe-keyspaces");

            // Expected Keyspaces : system, MuleSoft & ConnectorQA
            assertEquals(keyspaces.size(), 3);

            runFlowAndGetPayload("system-drop-keyspace");

            keyspaces = runFlowAndGetPayload("describe-keyspaces");
            // Expected Keyspaces : system & MuleSoft
            assertEquals(keyspaces.size(), 2);

        } catch (Exception e) {

            try {
                runFlowAndGetPayload("system-drop-keyspace");
            } catch (Exception e1) {

            }

            fail(ConnectorTestUtils.getStackTrace(e));

        }

    }
}

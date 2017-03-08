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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.tests.ConnectorTestUtils;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DescribeKeyspacesTestCases extends CassandraDBTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("describeKeyspacesTestData");
    }

    @Category({RegressionTests.class})
    @Test
    public void testDescribeKeyspaces() {
        try {

            List<KsDef> keyspaces = runFlowAndGetPayload("describe-keyspaces");
            // Expected Keyspaces : system & MuleSoft
            assertEquals(keyspaces.size(), 2);
            assertEquals(keyspaces.get(0).getName(), getTestRunMessageValue("keyspace-0"));
            assertEquals(keyspaces.get(1).getName(), getTestRunMessageValue("keyspace-1"));

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }
}

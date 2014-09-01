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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.tests.ConnectorTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DescribeVersionTestCases extends CassandraDBTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("describeVersionTestData");
    }

    @Category({RegressionTests.class})
    @Test
    public void testDescribeVersion() {
        try {

            String actualVersion = runFlowAndGetPayload("describe-version");
            assertEquals(getTestRunMessageValue("version"), actualVersion);

        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }
}

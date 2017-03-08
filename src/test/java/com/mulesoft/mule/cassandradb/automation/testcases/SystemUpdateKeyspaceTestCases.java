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

public class SystemUpdateKeyspaceTestCases extends CassandraDBTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("systemUpdateKeyspaceTestData");
        runFlowAndGetPayload("system-add-keyspace-with-params");
    }

    @Category({RegressionTests.class})
    @Test
    public void testSystemUpdateKeyspace() {
        try {
            KsDef keyspace = runFlowAndGetPayload("describe-keyspace");
            assertNotSame(keyspace.isDurable_writes(), Boolean.valueOf((String) getTestRunMessageValue("updateDurableWrites")));

            keyspace.setDurable_writes(Boolean.valueOf((String) getTestRunMessageValue("updateDurableWrites")));
            upsertOnTestRunMessage("ksDef", keyspace);

            String result = runFlowAndGetPayload("system-update-keyspace");
            assertNotNull(result);

            keyspace = runFlowAndGetPayload("describe-keyspace");
            assertEquals(keyspace.getName(), getTestRunMessageValue("keyspace"));
            assertEquals(keyspace.isDurable_writes(), Boolean.valueOf((String) getTestRunMessageValue("updateDurableWrites")));
        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("system-drop-keyspace");
    }
}

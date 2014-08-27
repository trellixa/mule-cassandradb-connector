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
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mule.modules.tests.ConnectorTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SystemDropColumnFamilyTestCases extends CassandraDBTestParent {

    @Before
    public void setUp() throws Exception {
        initializeTestRunMessage("systemDropColumnFamilyTestData");

        runFlowAndGetPayload("system-add-keyspace-with-params");

        runFlowAndGetPayload("system-add-column-family-with-params");
    }

    @Category({RegressionTests.class, SmokeTests.class})
    @Test
    public void testSystemDropColumnFamily() {
        try {
            KsDef keyspace = runFlowAndGetPayload("describe-keyspace");
            assertEquals(keyspace.getCf_defsSize(), 1);
            CfDef columnDef = keyspace.getCf_defs().get(0);
            assertEquals(columnDef.getName(), getTestRunMessageValue("columnFamily"));
            assertEquals(columnDef.getComparator_type(), getTestRunMessageValue("comparatorType"));
            assertEquals(columnDef.getKey_validation_class(), getTestRunMessageValue("keyValidationClass"));

            runFlowAndGetPayload("system-drop-column-family");

            keyspace = runFlowAndGetPayload("describe-keyspace");
            assertEquals(keyspace.getCf_defsSize(), 0);
        } catch (Exception e) {
            fail(ConnectorTestUtils.getStackTrace(e));
        }
    }

    @After
    public void tearDown() throws Exception {
        runFlowAndGetPayload("system-drop-keyspace");
    }
}

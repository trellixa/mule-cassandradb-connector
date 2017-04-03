/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.*;
import org.mule.common.bulk.BulkItem;
import org.mule.common.bulk.BulkOperationResult;
import org.mule.tools.devkit.ctf.mockup.ConnectorDispatcher;
import org.mule.tools.devkit.ctf.mockup.ConnectorTestContext;
import org.mule.util.StringUtils;

import com.google.common.base.Strings;
import com.mulesoft.mule.cassandradb.CassandraDBConnector;

public class AutomationTestParent {

    protected CassandraDBConnector connector;
    protected ConnectorDispatcher<CassandraDBConnector> dispatcher;

    @BeforeClass
    public static void beforeClassBase() throws Exception {
        if (Strings.isNullOrEmpty(System.getProperty("automation-credentials.properties"))) {
            System.setProperty("automation-credentials.properties", "basic-authentication.properties");
            System.setProperty("activeconfiguration", "basic-authentication-config");
        }

        ConnectorTestContext.initialize(CassandraDBConnector.class, false);
    }

    @Before
    public void beforeBase() throws Exception {
        ConnectorTestContext<CassandraDBConnector> context = ConnectorTestContext.getInstance();
        dispatcher = context.getConnectorDispatcher();
        connector = dispatcher.createMockup();

        this.beforeEachTest();
    }

    public void beforeEachTest() throws Exception {
        // Allow override for running code before each test
    }

    @After
    public void afterBase() throws Exception {
        this.afterEachTest();
    }

    public void afterEachTest() throws Exception {
        // Allow override for running code after each test
    }

    protected Map<String, Object> findInResults(List<Map<String, Object>> entities, String fieldName, String fieldValue) {
        for (Map<String, Object> entity : entities) {
            if (entity.containsKey(fieldName)) {
                if (entity.get(fieldName) != null) {
                    if (StringUtils.equalsIgnoreCase(entity.get(fieldName).toString(), fieldValue)) {
                        return entity;
                    }
                }
            }
        }
        return null;
    }

    protected List<String> getIds(BulkOperationResult<Map<String, Object>> results) {
        List<String> ids = new ArrayList<String>();
        for (BulkItem<Map<String, Object>> item : results.getItems()) {
            if (item.isSuccessful()) {
                ids.add(item.getId().toString());
            }
        }
        return ids;
    }

    protected <T> void failIfErrors(BulkOperationResult<T> results) {
        if (!results.isSuccessful()) {
            Assert.fail("At least one register was unsuccesful.");
        }
    }

    protected static CassandraDBConnector getConnector() {
        ConnectorTestContext<CassandraDBConnector> context = ConnectorTestContext.getInstance();
        ConnectorDispatcher<CassandraDBConnector> dispatcher = context.getConnectorDispatcher();
        return dispatcher.createMockup();
    }
}
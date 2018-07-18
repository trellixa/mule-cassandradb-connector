package org.mule.modules.cassandradb.automation.system;

import org.junit.Test;
import org.mule.modules.cassandradb.automation.functional.AbstractTestCases;
import org.mule.runtime.api.component.location.Location;
import org.mule.runtime.api.connection.ConnectionValidationResult;
import org.mule.runtime.api.connectivity.ConnectivityTestingService;

import javax.inject.Inject;
import javax.inject.Named;

import org.mule.runtime.api.connection.*;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.*;
import static org.mule.runtime.api.connectivity.ConnectivityTestingService.*;

public class ConfigTestCase extends AbstractTestCases {
    @Inject
    @Named(CONNECTIVITY_TESTING_SERVICE_KEY)
    ConnectivityTestingService connectivityTestingService;

    @Test
    public void connectWithBasicParamsTest() {
        ConnectionValidationResult validationResult = connectivityTestingService.testConnection(Location.builder()
                .globalName("Cassandra_Basic_Config").build());

        assertThat(validationResult.isValid(), is(true));
    }

    @Test
    public void connectWithAdvancedParamsTest() {
        ConnectionValidationResult validationResult = connectivityTestingService.testConnection(Location.builder()
                .globalName("Cassandra_Advanced_Config").build());

        assertThat(validationResult.isValid(), is(true));
    }

    @Test
    public void connectWithAdvancedParamsWrongClusterNodesNameTest() {
        ConnectionValidationResult validationResult = connectivityTestingService.testConnection(Location.builder()
                .globalName("Cassandra_Wrong_ClusterNodesName_Advanced_Config").build());
        assertThat(validationResult.getException(), is(instanceOf(ConnectionException.class)));
    }

    @Test
    public void connectWithBasicParamsWrongHostTest() {
        ConnectionValidationResult validationResult = connectivityTestingService.testConnection(Location.builder()
                .globalName("Cassandra_Wrong_Host_Basic_Config").build());
        assertThat(validationResult.getException(), is(instanceOf(ConnectionException.class)));
    }

    @Test
    public void connectWithBasicParamsWrongPortTest() {
        ConnectionValidationResult validationResult = connectivityTestingService.testConnection(Location.builder()
                .globalName("Cassandra_Wrong_Port_Basic_Config").build());
        assertThat(validationResult.getException(), is(instanceOf(ConnectionException.class)));
    }

    @Test
    public void connectWithBasicParamsWithNoPortTest() {
        ConnectionValidationResult validationResult = connectivityTestingService.testConnection(Location.builder()
                .globalName("Cassandra_No_Port_Basic_Config").build());
        assertThat(validationResult.getException(), is(instanceOf(ConnectionException.class)));
    }

    @Test
    public void connectWithBasicParamsWithNoHostTest() {
        ConnectionValidationResult validationResult = connectivityTestingService.testConnection(Location.builder()
                .globalName("Cassandra_No_Host_Basic_Config").build());
        assertThat(validationResult.getException(), is(instanceOf(ConnectionException.class)));
    }
}

package com.mulesoft.mule.cassandradb.automation.functional;

import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.Constants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import org.mule.api.ConnectionException;
import org.mule.tools.devkit.ctf.exceptions.ConfigurationLoadingFailedException;

import java.io.IOException;

public class InsertTestCases extends BaseTestCases {

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;

    @BeforeClass public static void setup() throws ConnectionException, CassandraDBException, IOException, ConfigurationLoadingFailedException {
        cassConfig = getClientConfig();
        cassClient = configureClient(cassConfig);
    }

    @AfterClass public static void tearDown() throws CassandraDBException, ConnectionException {
        cassClient.dropTable(Constants.TABLE_NAME, cassConfig.getKeyspace());
    }

    @Test public void testInsertWithSuccess() throws CassandraDBException {
        getConnector().insert(Constants.TABLE_NAME, TestDataBuilder.getValidEntity());
    }

    @Test(expected = CassandraDBException.class) public void testInsertWithInvalidInput() throws CassandraDBException {
        getConnector().insert(Constants.TABLE_NAME, TestDataBuilder.getInvalidEntity());
    }

}

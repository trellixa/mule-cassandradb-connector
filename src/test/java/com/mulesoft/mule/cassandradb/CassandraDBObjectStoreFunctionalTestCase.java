package com.mulesoft.mule.cassandradb;


import org.junit.Assert;
import org.junit.Test;
import org.mule.api.MuleMessage;
import org.mule.api.client.MuleClient;
import org.mule.api.store.PartitionableObjectStore;
import org.mule.tck.junit4.FunctionalTestCase;
import org.mule.util.UUID;

import java.io.Serializable;
import java.util.List;

import static org.junit.Assert.*;

public class CassandraDBObjectStoreFunctionalTestCase extends FunctionalTestCase {

    @Override
    protected String getConfigResources() {
        return "src/test/resources/functional/mule-object-store-config.xml";
    }

    @Test
    public void testGetAllKeys() throws Exception {

    }

    @Test
    public void testIdempotentReception() throws Exception {
        MuleClient client = muleContext.getClient();

        String payload = UUID.getUUID();
        client.dispatch("vm://in", payload, null);
        MuleMessage response = client.request("vm://out", 15000);
        assertNotNull(response);
        client.dispatch("vm://in", payload, null);
        // ToDo use latch here
        response = client.request("vm://out", 5000);
        assertNull(response);

        PartitionableObjectStore objectStore = muleContext.getRegistry().lookupObject("cassandraObjectStore");
        List<Serializable> keys = objectStore.allKeys();

        for (Serializable key : keys) {
            logger.info("KEY: " + objectStore.retrieve(key));
        }

        for (Serializable key : keys) {
            logger.info("REMOVING KEY: " + objectStore.remove(key));
        }

        logger.info("PARTITION NAMES: " + objectStore.allPartitions());
        logger.info("foo");
    }
}

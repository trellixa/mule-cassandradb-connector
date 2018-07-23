/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.connectors.cassandradb.internal.util;

import java.util.HashMap;
import java.util.Map;

public class ConnectionUtil {
    private ConnectionUtil() {
        // empty constructor
    }

    private static final String CASSANDRA_NODE_DEFAULT_PORT = "9042";

    public static Map<String,String> parseClusterNodesString(String clusterNodesString){
        Map<String,String> addresses = new HashMap<>();

        String[] parts = clusterNodesString.split(",");

        for(String part : parts) {
            if(part.contains(":")){
                String pair[] = part.split(":");

                addresses.put(pair[0].trim(), pair[1].trim());
            }

            else {
                addresses.put(part.trim(),CASSANDRA_NODE_DEFAULT_PORT);
            }
        }

        return addresses;
    }
}

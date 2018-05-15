package org.mule.modules.cassandradb.utils;

import java.util.HashMap;
import java.util.Map;

public class ConnectionUtil {
    public static Map<String,String> getAddress(String nodes){
        Map<String,String> addresses = new HashMap<>();

        String[] parts = nodes.split(",");

        for(String part : parts) {
            if(part.contains(":")){
                String pair[] = part.split(":");

                addresses.put(pair[0].trim(), pair[1].trim());
            }

            else {
                addresses.put(part.trim(),"9042");
            }
        }

        return addresses;
    }
}

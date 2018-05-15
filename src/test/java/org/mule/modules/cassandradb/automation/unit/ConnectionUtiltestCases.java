package org.mule.modules.cassandradb.automation.unit;

import org.junit.Test;
import org.mule.modules.cassandradb.utils.ConnectionUtil;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConnectionUtiltestCases {

    private Map<String, String> nodes = new HashMap<>();

    @Test
    public void testConnectionWithOneNode(){
       nodes = ConnectionUtil.getAddress("127.0.0.1:9042");

       assertThat(nodes.get("127.0.0.1"), is("9042"));
    }

    @Test
    public void testConnectionWithMultipleNodes(){
        nodes = ConnectionUtil.getAddress("127.0.0.1 : 9042, 127.0.0.2 : 9043, 127.0.0.3 : 9044");

        assertThat(nodes.get("127.0.0.1"), is("9042"));

        assertThat(nodes.get("127.0.0.2"), is("9043"));

        assertThat(nodes.get("127.0.0.3"), is("9044"));
    }

    @Test
    public void testConnectionWithMultipleNodesAndNoHosts(){
        nodes = ConnectionUtil.getAddress("127.0.0.1, 127.0.0.2, 127.0.0.3");

        assertThat(nodes.get("127.0.0.1"), is("9042"));

        assertThat(nodes.get("127.0.0.2"), is("9042"));

        assertThat(nodes.get("127.0.0.3"), is("9042"));
    }


    @Test
    public void testConnectionWithMultipleNodesAndOneHost(){
        nodes = ConnectionUtil.getAddress("127.0.0.1:9042, 127.0.0.2:9043, 127.0.0.3");

        assertThat(nodes.get("127.0.0.1"), is("9042"));

        assertThat(nodes.get("127.0.0.2"), is("9043"));

        assertThat(nodes.get("127.0.0.3"), is("9042"));
    }
}

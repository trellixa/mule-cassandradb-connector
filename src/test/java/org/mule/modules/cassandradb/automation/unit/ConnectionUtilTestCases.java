package org.mule.modules.cassandradb.automation.unit;

import org.junit.Test;
import org.mule.modules.cassandradb.utils.ConnectionUtil;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConnectionUtilTestCases {
    public static final String CASSANDRA_NODE_DEFAULT_PORT = "9042";

    private Map<String, String> nodes = new HashMap<>();

    @Test
    public void testConnectionWithOneNode(){
       nodes = ConnectionUtil.parseClusterNodesString("127.0.0.3:9044");

       assertThat(nodes.get("127.0.0.3"), is("9044"));
    }

    @Test
    public void testConnectionWithMultipleNodes(){
        nodes = ConnectionUtil.parseClusterNodesString("127.0.0.1 : 9042, 127.0.0.2 : 9043, 127.0.0.3 : 9044");

        assertThat(nodes.get("127.0.0.1"), is(CASSANDRA_NODE_DEFAULT_PORT));

        assertThat(nodes.get("127.0.0.2"), is("9043"));

        assertThat(nodes.get("127.0.0.3"), is("9044"));
    }

    @Test
    public void testConnectionWithMultipleNodesWithoutPorts(){
        nodes = ConnectionUtil.parseClusterNodesString("127.0.0.1, 127.0.0.2, 127.0.0.3");

        assertThat(nodes.get("127.0.0.1"), is(CASSANDRA_NODE_DEFAULT_PORT));

        assertThat(nodes.get("127.0.0.2"), is("9042"));

        assertThat(nodes.get("127.0.0.3"), is("9042"));
    }


    @Test
    public void testConnectionWithMultipleNodesAndNoPort(){
        nodes = ConnectionUtil.parseClusterNodesString("127.0.0.1:9042, 127.0.0.2:9043, 127.0.0.3");

        assertThat(nodes.get("127.0.0.1"), is(CASSANDRA_NODE_DEFAULT_PORT));

        assertThat(nodes.get("127.0.0.2"), is("9043"));

        assertThat(nodes.get("127.0.0.3"), is(CASSANDRA_NODE_DEFAULT_PORT));
    }
}

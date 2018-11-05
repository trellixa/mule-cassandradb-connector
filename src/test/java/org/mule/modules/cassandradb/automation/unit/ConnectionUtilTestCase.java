/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.unit;

import org.junit.Test;
import org.mule.modules.cassandradb.utils.ConnectionUtil;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConnectionUtilTestCase {
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

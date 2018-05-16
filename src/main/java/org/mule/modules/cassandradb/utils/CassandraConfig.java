/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.utils;

public class CassandraConfig {

    private String host;

    private String port;

    private String nodes;

    private String keyspace;

    public CassandraConfig(String host, String port, String nodes, String keyspace) {
        this.host = host;
        this.port = port;
        this.nodes = nodes;
        this.keyspace = keyspace;
    }

    public String getNodes() {
        return nodes;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }
}

/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.configurations;

public class ConnectionParameters {

    private String nodes;
    private String username;
    private String password;
    /*keyspace to retrieve cluster session for*/
    private String keyspace;
    private AdvancedConnectionParameters advancedConnectionParameters;

    public ConnectionParameters(String nodes, String username, String password, String keyspace, AdvancedConnectionParameters advancedConnectionParameters) {
        this.nodes = nodes;
        this.username = username;
        this.password = password;
        this.keyspace = keyspace;
        this.advancedConnectionParameters = advancedConnectionParameters;
    }

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public AdvancedConnectionParameters getAdvancedConnectionParameters() {
        return advancedConnectionParameters;
    }

    public void setAdvancedConnectionParameters(AdvancedConnectionParameters advancedConnectionParameters) {
        this.advancedConnectionParameters = advancedConnectionParameters;
    }
}

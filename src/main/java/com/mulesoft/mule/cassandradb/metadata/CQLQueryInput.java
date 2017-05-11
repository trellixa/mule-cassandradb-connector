/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.metadata;

import java.util.List;

public class CQLQueryInput {

    private String cqlQuery;
    private List<Object> parameters;

    public CQLQueryInput(String cqlQuery, List<Object> parameters) {
        this.cqlQuery = cqlQuery;
        this.parameters = parameters;
    }

    public CQLQueryInput() {
    }

    public String getCqlQuery() {
        return cqlQuery;
    }

    public void setCqlQuery(String cqlQuery) {
        this.cqlQuery = cqlQuery;
    }

    public List<Object> getParameters() {
        return parameters;
    }

    public void setParameters(List<Object> parameters) {
        this.parameters = parameters;
    }

    @Override
    public String toString() {
        return "CQLQueryInput{" +
                "cqlQuery='" + cqlQuery + '\'' +
                ", parameters=" + parameters +
                '}';
    }
}

/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.internal.connection;

import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;

public class AdvancedConnectionParameters {
    private ProtocolVersion protocolVersion;
    private String clusterName;
    private Integer maxSchemaAgreementWaitSeconds;
    private ProtocolOptions.Compression compression;
    private boolean ssl;

    public AdvancedConnectionParameters(ProtocolVersion protocolVersion, String clusterName, int maxSchemaAgreementWaitSeconds, ProtocolOptions.Compression compression,
                                        boolean ssl) {
        this.protocolVersion = protocolVersion;
        this.clusterName = clusterName;
        this.maxSchemaAgreementWaitSeconds = maxSchemaAgreementWaitSeconds;
        this.compression = compression;
        this.ssl = ssl;
    }

    public ProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    public String getClusterName() {
        return clusterName;
    }

    public Integer getMaxSchemaAgreementWaitSeconds() {
        return maxSchemaAgreementWaitSeconds;
    }

    public ProtocolOptions.Compression getCompression() {
        return compression;
    }

    public boolean isSsl() {
        return ssl;
    }
}

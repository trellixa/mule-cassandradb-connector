/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.connectors.cassandradb.internal.config;

import com.mulesoft.connectors.cassandradb.internal.connection.CassandraConnectionProvider;
import com.mulesoft.connectors.cassandradb.internal.operation.CassandraOperations;
import org.mule.connectors.commons.template.config.ConnectorConfig;
import org.mule.runtime.extension.api.annotation.Configuration;
import org.mule.runtime.extension.api.annotation.Operations;
import org.mule.runtime.extension.api.annotation.connectivity.ConnectionProviders;

@Configuration(name = "config")
@ConnectionProviders(CassandraConnectionProvider.class)
@Operations(CassandraOperations.class)
public class CassandraConfig implements ConnectorConfig {
}

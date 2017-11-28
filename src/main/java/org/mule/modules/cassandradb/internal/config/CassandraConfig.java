package org.mule.modules.cassandradb.internal.config;

import org.mule.modules.cassandradb.internal.connection.BasicAuthConnectionProvider;
import org.mule.runtime.extension.api.annotation.Configuration;
import org.mule.runtime.extension.api.annotation.connectivity.ConnectionProviders;

@Configuration(name = "config")
@ConnectionProviders({
        BasicAuthConnectionProvider.class
})
public class CassandraConfig {
}

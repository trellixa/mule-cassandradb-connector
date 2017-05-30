
package org.mule.modules.cassandradb.generated.connectivity;

import javax.annotation.Generated;
import org.mule.api.ConnectionException;
import org.mule.devkit.internal.connection.management.ConnectionManagementConnectionAdapter;
import org.mule.devkit.internal.connection.management.TestableConnection;
import org.mule.modules.cassandradb.configurations.BasicAuthConnectionStrategy;

@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.1", date = "2017-05-30T02:13:49-03:00", comments = "Build UNNAMED.2797.33dc1d7")
public class BasicAuthConnectionStrategyCassandraDBConnectorAdapter
    extends BasicAuthConnectionStrategy
    implements ConnectionManagementConnectionAdapter<BasicAuthConnectionStrategy, ConnectionManagementConfigCassandraDBConnectorConnectionKey> , TestableConnection<ConnectionManagementConfigCassandraDBConnectorConnectionKey>
{


    @Override
    public BasicAuthConnectionStrategy getStrategy() {
        return this;
    }

    @Override
    public void test(ConnectionManagementConfigCassandraDBConnectorConnectionKey connectionKey)
        throws ConnectionException
    {
        super.connect(connectionKey.getUsername(), connectionKey.getPassword());
    }

    @Override
    public void connect(ConnectionManagementConfigCassandraDBConnectorConnectionKey connectionKey)
        throws ConnectionException
    {
        super.connect(connectionKey.getUsername(), connectionKey.getPassword());
    }

    @Override
    public void disconnect() {
        super.disconnect();
    }

    @Override
    public String connectionId() {
        return super.connectionId();
    }

    @Override
    public boolean isConnected() {
        return super.isConnected();
    }

}

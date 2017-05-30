
package org.mule.modules.cassandradb.generated.connectivity;

import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ProtocolVersion;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Generated;
import org.apache.commons.pool.KeyedObjectPool;
import org.mule.api.MetadataAware;
import org.mule.api.MuleContext;
import org.mule.api.MuleEvent;
import org.mule.api.config.MuleProperties;
import org.mule.api.context.MuleContextAware;
import org.mule.api.devkit.ProcessAdapter;
import org.mule.api.devkit.ProcessTemplate;
import org.mule.api.devkit.capability.Capabilities;
import org.mule.api.devkit.capability.ModuleCapability;
import org.mule.api.lifecycle.Disposable;
import org.mule.api.lifecycle.Initialisable;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.retry.RetryPolicyTemplate;
import org.mule.common.DefaultResult;
import org.mule.common.DefaultTestResult;
import org.mule.common.Result;
import org.mule.common.TestResult;
import org.mule.common.Testable;
import org.mule.common.metadata.ConnectorMetaDataEnabled;
import org.mule.common.metadata.DefaultMetaDataKey;
import org.mule.common.metadata.MetaData;
import org.mule.common.metadata.MetaDataFailureType;
import org.mule.common.metadata.MetaDataKey;
import org.mule.common.metadata.NativeQueryMetadataTranslator;
import org.mule.common.metadata.key.property.TypeDescribingProperty;
import org.mule.common.metadata.property.StructureIdentifierMetaDataModelProperty;
import org.mule.common.query.DsqlQuery;
import org.mule.config.PoolingProfile;
import org.mule.devkit.api.exception.ConfigurationWarning;
import org.mule.devkit.api.lifecycle.LifeCycleManager;
import org.mule.devkit.api.lifecycle.MuleContextAwareManager;
import org.mule.devkit.internal.connection.management.ConnectionManagementConnectionAdapter;
import org.mule.devkit.internal.connection.management.ConnectionManagementConnectionManager;
import org.mule.devkit.internal.connection.management.ConnectionManagementConnectorAdapter;
import org.mule.devkit.internal.connection.management.ConnectionManagementConnectorFactory;
import org.mule.devkit.internal.connection.management.ConnectionManagementProcessTemplate;
import org.mule.devkit.internal.connectivity.ConnectivityTestingErrorHandler;
import org.mule.devkit.internal.metadata.MetaDataGeneratorUtils;
import org.mule.devkit.processor.ExpressionEvaluatorSupport;
import org.mule.modules.cassandradb.CassandraDBConnector;
import org.mule.modules.cassandradb.configurations.BasicAuthConnectionStrategy;
import org.mule.modules.cassandradb.generated.adapters.CassandraDBConnectorConnectionManagementAdapter;
import org.mule.modules.cassandradb.generated.pooling.DevkitGenericKeyedObjectPool;
import org.mule.modules.cassandradb.metadata.CassandraMetadataCategory;
import org.mule.modules.cassandradb.metadata.CassandraOnlyWithFiltersMetadataCategory;
import org.mule.modules.cassandradb.metadata.CassandraWithFiltersMetadataCategory;


/**
 * A {@code CassandraDBConnectorConfigConnectionManagementConnectionManager} is a wrapper around {@link CassandraDBConnector } that adds connection management capabilities to the pojo.
 * 
 */
@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.1", date = "2017-05-30T02:13:49-03:00", comments = "Build UNNAMED.2797.33dc1d7")
public class CassandraDBConnectorConfigConnectionManagementConnectionManager
    extends ExpressionEvaluatorSupport
    implements MetadataAware, MuleContextAware, ProcessAdapter<CassandraDBConnectorConnectionManagementAdapter> , Capabilities, Disposable, Initialisable, Testable, ConnectorMetaDataEnabled, NativeQueryMetadataTranslator, ConnectionManagementConnectionManager<ConnectionManagementConfigCassandraDBConnectorConnectionKey, CassandraDBConnectorConnectionManagementAdapter, BasicAuthConnectionStrategy>
{

    /**
     * 
     */
    private String username;
    /**
     * 
     */
    private String password;
    private String host;
    private String port;
    private String keyspace;
    private String clusterName;
    private ProtocolVersion protocolVersion;
    private int maxSchemaAgreementWaitSeconds;
    private Compression compression;
    private boolean sslEnabled;
    /**
     * Mule Context
     * 
     */
    protected MuleContext muleContext;
    /**
     * Connector Pool
     * 
     */
    private KeyedObjectPool connectionPool;
    protected PoolingProfile poolingProfile;
    protected RetryPolicyTemplate retryPolicyTemplate;
    private final static String MODULE_NAME = "CassandraDB";
    private final static String MODULE_VERSION = "2.0.0";
    private final static String DEVKIT_VERSION = "3.9.1";
    private final static String DEVKIT_BUILD = "UNNAMED.2797.33dc1d7";
    private final static String MIN_MULE_VERSION = "3.6";

    /**
     * Sets username
     * 
     * @param value Value to set
     */
    public void setUsername(String value) {
        this.username = value;
    }

    /**
     * Retrieves username
     * 
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * Sets password
     * 
     * @param value Value to set
     */
    public void setPassword(String value) {
        this.password = value;
    }

    /**
     * Retrieves password
     * 
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * Sets host
     * 
     * @param value Value to set
     */
    public void setHost(String value) {
        this.host = value;
    }

    /**
     * Retrieves host
     * 
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Sets port
     * 
     * @param value Value to set
     */
    public void setPort(String value) {
        this.port = value;
    }

    /**
     * Retrieves port
     * 
     */
    public String getPort() {
        return this.port;
    }

    /**
     * Sets keyspace
     * 
     * @param value Value to set
     */
    public void setKeyspace(String value) {
        this.keyspace = value;
    }

    /**
     * Retrieves keyspace
     * 
     */
    public String getKeyspace() {
        return this.keyspace;
    }

    /**
     * Sets clusterName
     * 
     * @param value Value to set
     */
    public void setClusterName(String value) {
        this.clusterName = value;
    }

    /**
     * Retrieves clusterName
     * 
     */
    public String getClusterName() {
        return this.clusterName;
    }

    /**
     * Sets protocolVersion
     * 
     * @param value Value to set
     */
    public void setProtocolVersion(ProtocolVersion value) {
        this.protocolVersion = value;
    }

    /**
     * Retrieves protocolVersion
     * 
     */
    public ProtocolVersion getProtocolVersion() {
        return this.protocolVersion;
    }

    /**
     * Sets maxSchemaAgreementWaitSeconds
     * 
     * @param value Value to set
     */
    public void setMaxSchemaAgreementWaitSeconds(int value) {
        this.maxSchemaAgreementWaitSeconds = value;
    }

    /**
     * Retrieves maxSchemaAgreementWaitSeconds
     * 
     */
    public int getMaxSchemaAgreementWaitSeconds() {
        return this.maxSchemaAgreementWaitSeconds;
    }

    /**
     * Sets compression
     * 
     * @param value Value to set
     */
    public void setCompression(Compression value) {
        this.compression = value;
    }

    /**
     * Retrieves compression
     * 
     */
    public Compression getCompression() {
        return this.compression;
    }

    /**
     * Sets sslEnabled
     * 
     * @param value Value to set
     */
    public void setSslEnabled(boolean value) {
        this.sslEnabled = value;
    }

    /**
     * Retrieves sslEnabled
     * 
     */
    public boolean getSslEnabled() {
        return this.sslEnabled;
    }

    /**
     * Sets muleContext
     * 
     * @param value Value to set
     */
    public void setMuleContext(MuleContext value) {
        this.muleContext = value;
    }

    /**
     * Retrieves muleContext
     * 
     */
    public MuleContext getMuleContext() {
        return this.muleContext;
    }

    /**
     * Sets poolingProfile
     * 
     * @param value Value to set
     */
    public void setPoolingProfile(PoolingProfile value) {
        this.poolingProfile = value;
    }

    /**
     * Retrieves poolingProfile
     * 
     */
    public PoolingProfile getPoolingProfile() {
        return this.poolingProfile;
    }

    /**
     * Sets retryPolicyTemplate
     * 
     * @param value Value to set
     */
    public void setRetryPolicyTemplate(RetryPolicyTemplate value) {
        this.retryPolicyTemplate = value;
    }

    /**
     * Retrieves retryPolicyTemplate
     * 
     */
    public RetryPolicyTemplate getRetryPolicyTemplate() {
        return this.retryPolicyTemplate;
    }

    public void initialise() {
        connectionPool = new DevkitGenericKeyedObjectPool(new ConnectionManagementConnectorFactory(this), poolingProfile);
        if (retryPolicyTemplate == null) {
            retryPolicyTemplate = muleContext.getRegistry().lookupObject(MuleProperties.OBJECT_DEFAULT_RETRY_POLICY_TEMPLATE);
        }
    }

    @Override
    public void dispose() {
        try {
            connectionPool.close();
        } catch (Exception e) {
        }
    }

    public CassandraDBConnectorConnectionManagementAdapter acquireConnection(ConnectionManagementConfigCassandraDBConnectorConnectionKey key)
        throws Exception
    {
        return ((CassandraDBConnectorConnectionManagementAdapter) connectionPool.borrowObject(key));
    }

    public void releaseConnection(ConnectionManagementConfigCassandraDBConnectorConnectionKey key, CassandraDBConnectorConnectionManagementAdapter connection)
        throws Exception
    {
        connectionPool.returnObject(key, connection);
    }

    public void destroyConnection(ConnectionManagementConfigCassandraDBConnectorConnectionKey key, CassandraDBConnectorConnectionManagementAdapter connection)
        throws Exception
    {
        connectionPool.invalidateObject(key, connection);
    }

    /**
     * Returns true if this module implements such capability
     * 
     */
    public boolean isCapableOf(ModuleCapability capability) {
        if (capability == ModuleCapability.LIFECYCLE_CAPABLE) {
            return true;
        }
        if (capability == ModuleCapability.CONNECTION_MANAGEMENT_CAPABLE) {
            return true;
        }
        return false;
    }

    @Override
    public<P >ProcessTemplate<P, CassandraDBConnectorConnectionManagementAdapter> getProcessTemplate() {
        return new ConnectionManagementProcessTemplate(this, muleContext);
    }

    @Override
    public ConnectionManagementConfigCassandraDBConnectorConnectionKey getDefaultConnectionKey() {
        return new ConnectionManagementConfigCassandraDBConnectorConnectionKey(getUsername(), getPassword());
    }

    @Override
    public ConnectionManagementConfigCassandraDBConnectorConnectionKey getEvaluatedConnectionKey(MuleEvent event)
        throws Exception
    {
        if (event!= null) {
            final String _transformedUsername = ((String) evaluateAndTransform(muleContext, event, this.getClass().getDeclaredField("username").getGenericType(), null, getUsername()));
            final String _transformedPassword = ((String) evaluateAndTransform(muleContext, event, this.getClass().getDeclaredField("password").getGenericType(), null, getPassword()));
            return new ConnectionManagementConfigCassandraDBConnectorConnectionKey(_transformedUsername, _transformedPassword);
        }
        return getDefaultConnectionKey();
    }

    public String getModuleName() {
        return MODULE_NAME;
    }

    public String getModuleVersion() {
        return MODULE_VERSION;
    }

    public String getDevkitVersion() {
        return DEVKIT_VERSION;
    }

    public String getDevkitBuild() {
        return DEVKIT_BUILD;
    }

    public String getMinMuleVersion() {
        return MIN_MULE_VERSION;
    }

    @Override
    public ConnectionManagementConfigCassandraDBConnectorConnectionKey getConnectionKey(MessageProcessor messageProcessor, MuleEvent event)
        throws Exception
    {
        return getEvaluatedConnectionKey(event);
    }

    @Override
    public ConnectionManagementConnectionAdapter newConnection() {
        BasicAuthConnectionStrategyCassandraDBConnectorAdapter connection = new BasicAuthConnectionStrategyCassandraDBConnectorAdapter();
        connection.setHost(getHost());
        connection.setPort(getPort());
        connection.setKeyspace(getKeyspace());
        connection.setClusterName(getClusterName());
        connection.setProtocolVersion(getProtocolVersion());
        connection.setMaxSchemaAgreementWaitSeconds(getMaxSchemaAgreementWaitSeconds());
        connection.setCompression(getCompression());
        connection.setSslEnabled(getSslEnabled());
        return connection;
    }

    @Override
    public ConnectionManagementConnectorAdapter newConnector(ConnectionManagementConnectionAdapter<BasicAuthConnectionStrategy, ConnectionManagementConfigCassandraDBConnectorConnectionKey> connection) {
        CassandraDBConnectorConnectionManagementAdapter connector = new CassandraDBConnectorConnectionManagementAdapter();
        connector.setBasicAuthConnectionStrategy(connection.getStrategy());
        return connector;
    }

    public ConnectionManagementConnectionAdapter getConnectionAdapter(ConnectionManagementConnectorAdapter adapter) {
        CassandraDBConnectorConnectionManagementAdapter connector = ((CassandraDBConnectorConnectionManagementAdapter) adapter);
        ConnectionManagementConnectionAdapter strategy = ((ConnectionManagementConnectionAdapter) connector.getBasicAuthConnectionStrategy());
        return strategy;
    }

    public TestResult test() {
        try {
            BasicAuthConnectionStrategyCassandraDBConnectorAdapter strategy = ((BasicAuthConnectionStrategyCassandraDBConnectorAdapter) newConnection());
            MuleContextAwareManager.setMuleContext(strategy, this.muleContext);
            LifeCycleManager.executeInitialiseAndStart(strategy);
            ConnectionManagementConnectorAdapter connectorAdapter = newConnector(strategy);
            MuleContextAwareManager.setMuleContext(connectorAdapter, this.muleContext);
            LifeCycleManager.executeInitialiseAndStart(connectorAdapter);
            strategy.test(getDefaultConnectionKey());
            return new DefaultTestResult(Result.Status.SUCCESS);
        } catch (ConfigurationWarning warning) {
            return ((DefaultTestResult) ConnectivityTestingErrorHandler.buildWarningTestResult(warning));
        } catch (Exception e) {
            return ((DefaultTestResult) ConnectivityTestingErrorHandler.buildFailureTestResult(e));
        }
    }

    @Override
    public Result<List<MetaDataKey>> getMetaDataKeys() {
        CassandraDBConnectorConnectionManagementAdapter connection = null;
        ConnectionManagementConfigCassandraDBConnectorConnectionKey key = getDefaultConnectionKey();
        try {
            connection = acquireConnection(key);
            try {
                List<MetaDataKey> gatheredMetaDataKeys = new ArrayList<MetaDataKey>();
                CassandraMetadataCategory cassandraMetadataCategory = new CassandraMetadataCategory();
                cassandraMetadataCategory.setCassandraConnector(connection);
                gatheredMetaDataKeys.addAll(MetaDataGeneratorUtils.fillCategory(cassandraMetadataCategory.getMetadataKeys(), "CassandraMetadataCategory"));
                CassandraWithFiltersMetadataCategory cassandraWithFiltersMetadataCategory = new CassandraWithFiltersMetadataCategory();
                cassandraWithFiltersMetadataCategory.setCassandraConnector(connection);
                gatheredMetaDataKeys.addAll(MetaDataGeneratorUtils.fillCategory(cassandraWithFiltersMetadataCategory.getMetadataKeys(), "CassandraWithFiltersMetadataCategory"));
                CassandraOnlyWithFiltersMetadataCategory cassandraOnlyWithFiltersMetadataCategory = new CassandraOnlyWithFiltersMetadataCategory();
                cassandraOnlyWithFiltersMetadataCategory.setCassandraConnector(connection);
                gatheredMetaDataKeys.addAll(MetaDataGeneratorUtils.fillCategory(cassandraOnlyWithFiltersMetadataCategory.getMetadataKeys(), "CassandraOnlyWithFiltersMetadataCategory"));
                return new DefaultResult<List<MetaDataKey>>(gatheredMetaDataKeys, (Result.Status.SUCCESS));
            } catch (Exception e) {
                return new DefaultResult<List<MetaDataKey>>(null, (Result.Status.FAILURE), "There was an error retrieving the metadata keys from service provider after acquiring connection, for more detailed information please read the provided stacktrace", MetaDataFailureType.ERROR_METADATA_KEYS_RETRIEVER, e);
            }
        } catch (Exception e) {
            try {
                destroyConnection(key, connection);
            } catch (Exception ie) {
            }
            return ((DefaultResult<List<MetaDataKey>> ) ConnectivityTestingErrorHandler.buildFailureTestResult(e));
        } finally {
            if (connection!= null) {
                try {
                    releaseConnection(key, connection);
                } catch (Exception ie) {
                }
            }
        }
    }

    @Override
    public Result<MetaData> getMetaData(MetaDataKey metaDataKey) {
        CassandraDBConnectorConnectionManagementAdapter connection = null;
        ConnectionManagementConfigCassandraDBConnectorConnectionKey key = getDefaultConnectionKey();
        try {
            connection = acquireConnection(key);
            try {
                MetaData metaData = null;
                TypeDescribingProperty property = metaDataKey.getProperty(TypeDescribingProperty.class);
                String category = ((DefaultMetaDataKey) metaDataKey).getCategory();
                if (category!= null) {
                    if (category.equals("CassandraMetadataCategory")) {
                        CassandraMetadataCategory cassandraMetadataCategory = new CassandraMetadataCategory();
                        cassandraMetadataCategory.setCassandraConnector(connection);
                        metaData = cassandraMetadataCategory.getInputMetaData(metaDataKey);
                    } else {
                        if (category.equals("CassandraWithFiltersMetadataCategory")) {
                            CassandraWithFiltersMetadataCategory cassandraWithFiltersMetadataCategory = new CassandraWithFiltersMetadataCategory();
                            cassandraWithFiltersMetadataCategory.setCassandraConnector(connection);
                            metaData = cassandraWithFiltersMetadataCategory.getInputMetaData(metaDataKey);
                        } else {
                            if (category.equals("CassandraOnlyWithFiltersMetadataCategory")) {
                                CassandraOnlyWithFiltersMetadataCategory cassandraOnlyWithFiltersMetadataCategory = new CassandraOnlyWithFiltersMetadataCategory();
                                cassandraOnlyWithFiltersMetadataCategory.setCassandraConnector(connection);
                                metaData = cassandraOnlyWithFiltersMetadataCategory.getInputMetaData(metaDataKey);
                            } else {
                                throw new Exception(((("Invalid key type. There is no matching category for ["+ metaDataKey.getId())+"]. All keys must contain a category with any of the following options:[CassandraMetadataCategory, CassandraWithFiltersMetadataCategory, CassandraOnlyWithFiltersMetadataCategory]")+((", but found ["+ category)+"] instead")));
                            }
                        }
                    }
                } else {
                    throw new Exception((("Invalid key type. There is no matching category for ["+ metaDataKey.getId())+"]. All keys must contain a category with any of the following options:[CassandraMetadataCategory, CassandraWithFiltersMetadataCategory, CassandraOnlyWithFiltersMetadataCategory]"));
                }
                metaData.getPayload().addProperty(new StructureIdentifierMetaDataModelProperty(metaDataKey, false));
                return new DefaultResult<MetaData>(metaData);
            } catch (Exception e) {
                return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), MetaDataGeneratorUtils.getMetaDataException(metaDataKey), MetaDataFailureType.ERROR_METADATA_RETRIEVER, e);
            }
        } catch (Exception e) {
            try {
                destroyConnection(key, connection);
            } catch (Exception ie) {
            }
            return ((DefaultResult<MetaData> ) ConnectivityTestingErrorHandler.buildFailureTestResult(e));
        } finally {
            if (connection!= null) {
                try {
                    releaseConnection(key, connection);
                } catch (Exception ie) {
                }
            }
        }
    }

    @Override
    public Result<String> toNativeQuery(DsqlQuery query) {
        CassandraDBConnectorConnectionManagementAdapter connection = null;
        Result<String> result;
        ConnectionManagementConfigCassandraDBConnectorConnectionKey key = getDefaultConnectionKey();
        try {
            connection = acquireConnection(key);
            result = new DefaultResult<String>(connection.toNativeQuery(query).toString());
        } catch (Exception e) {
            try {
                destroyConnection(key, connection);
            } catch (Exception ie) {
            }
            result = new DefaultResult<String>(null, Result.Status.FAILURE, e.getMessage());
        } finally {
            if (connection!= null) {
                try {
                    releaseConnection(key, connection);
                } catch (Exception ie) {
                }
            }
        }
        return result;
    }

}

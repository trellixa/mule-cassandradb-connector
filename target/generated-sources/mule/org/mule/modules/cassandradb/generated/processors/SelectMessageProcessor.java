
package org.mule.modules.cassandradb.generated.processors;

import java.util.List;
import javax.annotation.Generated;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.config.ConfigurationException;
import org.mule.api.devkit.ProcessAdapter;
import org.mule.api.devkit.ProcessTemplate;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.registry.RegistrationException;
import org.mule.common.DefaultResult;
import org.mule.common.FailureType;
import org.mule.common.Result;
import org.mule.common.metadata.ConnectorMetaDataEnabled;
import org.mule.common.metadata.DefaultListMetaDataModel;
import org.mule.common.metadata.DefaultMetaData;
import org.mule.common.metadata.DefaultMetaDataKey;
import org.mule.common.metadata.DefaultPojoMetaDataModel;
import org.mule.common.metadata.DefaultSimpleMetaDataModel;
import org.mule.common.metadata.MetaData;
import org.mule.common.metadata.MetaDataKey;
import org.mule.common.metadata.MetaDataModel;
import org.mule.common.metadata.OperationMetaDataEnabled;
import org.mule.common.metadata.datatype.DataType;
import org.mule.common.metadata.datatype.DataTypeFactory;
import org.mule.common.metadata.key.property.TypeDescribingProperty;
import org.mule.common.metadata.util.MetaDataQueryFilter;
import org.mule.common.query.DsqlQuery;
import org.mule.common.query.dsql.parser.MuleDsqlParser;
import org.mule.devkit.internal.dsql.DsqlMelParserUtils;
import org.mule.devkit.internal.metadata.MetaDataGeneratorUtils;
import org.mule.devkit.internal.metadata.fixes.STUDIO7157;
import org.mule.devkit.internal.metadata.property.key.NativeQueryKeyProperty;
import org.mule.devkit.processor.DevkitBasedMessageProcessor;
import org.mule.modules.cassandradb.CassandraDBConnector;
import org.mule.security.oauth.callback.ProcessCallback;


/**
 * SelectMessageProcessor invokes the {@link org.mule.modules.cassandradb.CassandraDBConnector#select(java.lang.String, java.util.List)} method in {@link CassandraDBConnector }. For each argument there is a field in this processor to match it.  Before invoking the actual method the processor will evaluate and transform where possible to the expected argument type.
 * 
 */
@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.1", date = "2017-05-30T02:13:49-03:00", comments = "Build UNNAMED.2797.33dc1d7")
public class SelectMessageProcessor
    extends DevkitBasedMessageProcessor
    implements MessageProcessor, OperationMetaDataEnabled
{

    protected Object query;
    protected String _queryType;
    protected Object parameters;
    protected List<Object> _parametersType;

    public SelectMessageProcessor(String operationName) {
        super(operationName);
    }

    /**
     * Obtains the expression manager from the Mule context and initialises the connector. If a target object  has not been set already it will search the Mule registry for a default one.
     * 
     * @throws InitialisationException
     */
    public void initialise()
        throws InitialisationException
    {
    }

    @Override
    public void start()
        throws MuleException
    {
        super.start();
    }

    @Override
    public void stop()
        throws MuleException
    {
        super.stop();
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    /**
     * Sets query
     * 
     * @param value Value to set
     */
    public void setQuery(Object value) {
        this.query = value;
    }

    /**
     * Sets parameters
     * 
     * @param value Value to set
     */
    public void setParameters(Object value) {
        this.parameters = value;
    }

    /**
     * Invokes the MessageProcessor.
     * 
     * @param event MuleEvent to be processed
     * @throws Exception
     */
    public MuleEvent doProcess(final MuleEvent event)
        throws Exception
    {
        Object moduleObject = null;
        try {
            moduleObject = findOrCreate(null, false, event);
            DsqlMelParserUtils dsqlParserQuery = new DsqlMelParserUtils();
            final String _transformedQuery = ((String) evaluateAndTransform(getMuleContext(), event, SelectMessageProcessor.class.getDeclaredField("_queryType").getGenericType(), null, dsqlParserQuery.parseDsql(getMuleContext(), event, query)));
            final List<Object> _transformedParameters = ((List<Object> ) evaluateAndTransform(getMuleContext(), event, SelectMessageProcessor.class.getDeclaredField("_parametersType").getGenericType(), null, parameters));
            Object resultPayload;
            final ProcessTemplate<Object, Object> processTemplate = ((ProcessAdapter<Object> ) moduleObject).getProcessTemplate();
            resultPayload = processTemplate.execute(new ProcessCallback<Object,Object>() {


                public List<Class<? extends Exception>> getManagedExceptions() {
                    return null;
                }

                public boolean isProtected() {
                    return false;
                }

                public Object process(Object object)
                    throws Exception
                {
                    CassandraDBConnector connector = ((CassandraDBConnector) object);
                    String trimmedQuery = (_transformedQuery);
                    if ((trimmedQuery!= null)&&(_transformedQuery).startsWith("dsql:")) {
                        trimmedQuery = (_transformedQuery).substring(5);
                        MuleDsqlParser parser = new MuleDsqlParser();
                        DsqlQuery q = parser.parse(trimmedQuery);
                        return ((CassandraDBConnector) object).select(connector.toNativeQuery(q).toString(), _transformedParameters);
                    } else {
                        return ((CassandraDBConnector) object).select(_transformedQuery, _transformedParameters);
                    }
                }

            }
            , this, event);
            event.getMessage().setPayload(resultPayload);
            return event;
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public Result<MetaData> getInputMetaData() {
        MetaDataModel metaDataPayload = getPojoOrSimpleModel(String.class);
        DefaultMetaDataKey keyForStudio = new DefaultMetaDataKey("INPUT_METADATA", null);
        keyForStudio.setCategory("CassandraMetadataCategory");
        metaDataPayload.addProperty(STUDIO7157 .getStructureIdentifierMetaDataModelProperty(keyForStudio, false, false));
        return new DefaultResult<MetaData>(new DefaultMetaData(metaDataPayload));
    }

    @Override
    public Result<MetaData> getOutputMetaData(MetaData inputMetadata) {
        try {
            MuleDsqlParser parser = new MuleDsqlParser();
            String queryStr = ((String)(query));
            MetaData metaData = null;
            Result<MetaData> result = null;
            if ((queryStr!= null)&&queryStr.startsWith("dsql:")) {
                queryStr = queryStr.substring(5);
                DsqlQuery q = parser.parse(queryStr);
                result = auxOutputMetaData(null, q.getTypes().get(0).getName(), false);
                metaData = new MetaDataQueryFilter(result.get(), q.getFields()).doFilter();
            } else {
                return new DefaultResult<MetaData>(null, (Result.Status.FAILURE));
            }
            if (metaData!= null) {
                metaData.getPayload().addProperty(STUDIO7157 .getStructureIdentifierMetaDataModelProperty(null, true, true));
                return new DefaultResult<MetaData>(metaData);
            } else {
                return result;
            }
        } catch (Exception e) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), "Failed on parsing and getting query metadata");
        }
    }

    public Result<MetaData> auxOutputMetaData(MetaData inputMetadata, String key, Boolean nativeQuery) {
        if (((key) == null)||((key).toString() == null)) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), "There was an error retrieving metadata from parameter: key at processor select at module CassandraDBConnector");
        }
        DefaultMetaDataKey metaDataKey = new DefaultMetaDataKey((key).toString(), null);
        metaDataKey.setCategory("CassandraMetadataCategory");
        metaDataKey.addProperty(new TypeDescribingProperty(TypeDescribingProperty.TypeScope.OUTPUT, "select"));
        if (nativeQuery) {
            metaDataKey.addProperty(new NativeQueryKeyProperty());
        }
        Result<MetaData> genericMetaData = getGenericMetaData(metaDataKey);
        if ((Result.Status.FAILURE).equals(genericMetaData.getStatus())) {
            return genericMetaData;
        }
        MetaDataModel metaDataPayload = genericMetaData.get().getPayload();
        DefaultMetaDataKey keyForStudio = new DefaultMetaDataKey((key).toString(), null);
        keyForStudio.setCategory("CassandraMetadataCategory");
        metaDataPayload.addProperty(STUDIO7157 .getStructureIdentifierMetaDataModelProperty(keyForStudio, false, false));
        MetaDataModel wrappedMetaDataModel = new DefaultListMetaDataModel(metaDataPayload);
        return new DefaultResult<MetaData>(MetaDataGeneratorUtils.extractPropertiesToMetaData(wrappedMetaDataModel, genericMetaData.get()));
    }

    private MetaDataModel getPojoOrSimpleModel(Class clazz) {
        DataType dataType = DataTypeFactory.getInstance().getDataType(clazz);
        if (DataType.POJO.equals(dataType)) {
            return new DefaultPojoMetaDataModel(clazz);
        } else {
            return new DefaultSimpleMetaDataModel(dataType);
        }
    }

    public Result<MetaData> getGenericMetaData(MetaDataKey metaDataKey) {
        ConnectorMetaDataEnabled connector;
        try {
            connector = ((ConnectorMetaDataEnabled) findOrCreate(null, false, null));
            try {
                Result<MetaData> metadata = connector.getMetaData(metaDataKey);
                if ((Result.Status.FAILURE).equals(metadata.getStatus())) {
                    return metadata;
                }
                if (metadata.get() == null) {
                    return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), "There was an error processing metadata at CassandraDBConnector at select retrieving was successful but result is null");
                }
                return metadata;
            } catch (Exception e) {
                return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), e.getMessage(), FailureType.UNSPECIFIED, e);
            }
        } catch (ClassCastException cast) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), "There was an error getting metadata, there was no connection manager available. Maybe you're trying to use metadata from an Oauth connector");
        } catch (ConfigurationException e) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), e.getMessage(), FailureType.UNSPECIFIED, e);
        } catch (RegistrationException e) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), e.getMessage(), FailureType.UNSPECIFIED, e);
        } catch (IllegalAccessException e) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), e.getMessage(), FailureType.UNSPECIFIED, e);
        } catch (InstantiationException e) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), e.getMessage(), FailureType.UNSPECIFIED, e);
        } catch (Exception e) {
            return new DefaultResult<MetaData>(null, (Result.Status.FAILURE), e.getMessage(), FailureType.UNSPECIFIED, e);
        }
    }

}

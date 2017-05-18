/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.metadata;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;
import org.mule.modules.cassandradb.CassandraDBConnector;
import org.mule.modules.cassandradb.api.CassandraClient;
import org.apache.commons.collections.CollectionUtils;
import org.mule.api.annotations.MetaDataKeyRetriever;
import org.mule.api.annotations.MetaDataRetriever;
import org.mule.api.annotations.components.MetaDataCategory;
import org.mule.common.metadata.*;
import org.mule.common.metadata.builder.DefaultMetaDataBuilder;
import org.mule.common.metadata.builder.DynamicObjectBuilder;
import org.mule.common.metadata.datatype.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

import static org.mule.common.metadata.datatype.DataType.*;

@MetaDataCategory
public class CassandraMetadataCategory {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraMetadataCategory.class);

    @Inject
    private CassandraDBConnector cassandraConnector;

    /**
     * @return a list of {@link MetaDataKey} or empty list otherwise
     */
    @MetaDataKeyRetriever
    public List<MetaDataKey> getMetadataKeys() {
        LOGGER.info("Retrieving metadata keys...");
        List<MetaDataKey> keys = new ArrayList<MetaDataKey>();
        final String keyspaceUsed = cassandraConnector.getBasicAuthConnectionStrategy().getCassandraClient().getLoggedKeyspace();
        final CassandraClient cassandraClient = cassandraConnector.getBasicAuthConnectionStrategy().getCassandraClient();

        //extract metadata from database
        List<String> keyNames = cassandraClient.getTableNamesFromKeyspace(keyspaceUsed);

        //build the metadata
        if (CollectionUtils.isNotEmpty(keyNames)) {
            for (String type : keyNames) {
                final DefaultMetaDataKey dataKey = new DefaultMetaDataKey(type, type);
                keys.add(dataKey);
            }
        }

        return keys;
    }

    /**
     * @param key the metadata key to build the info for
     * @return {@link MetaData} for the given {@link MetaDataKey key}.
     */
    @MetaDataRetriever
    public MetaData getInputMetaData(final MetaDataKey key) {

        TableMetadata tableMetadata = getTableMetadata(key);
        //build the metadata
        if (tableMetadata != null && tableMetadata.getColumns() != null) {
            DynamicObjectBuilder<?> listEntityModel = new DefaultMetaDataBuilder().createList().ofDynamicObject(tableMetadata.getName());

            for (ColumnMetadata column : tableMetadata.getColumns()) {
                addMetadataField(listEntityModel, column);
            }

            return new DefaultMetaData(listEntityModel.build());
        }

        return new DefaultMetaData(null);
    }

    protected TableMetadata getTableMetadata(final MetaDataKey key) {
        LOGGER.info("Retrieving input metadata for the key: {}", key);
        final String keyspaceUsed = cassandraConnector.getBasicAuthConnectionStrategy().getCassandraClient().getLoggedKeyspace();
        final CassandraClient cassandraClient = cassandraConnector.getBasicAuthConnectionStrategy().getCassandraClient();

        //extract tables metadata from database
        return cassandraClient.fetchTableMetadata(keyspaceUsed, key.getId());
    }

    protected void addMetadataField(DynamicObjectBuilder<?> listEntityModel, ColumnMetadata column) {

        org.mule.common.metadata.datatype.DataType columnDataType = resolveDataType(column.getType());

        switch (columnDataType) {
            case LIST:
                DataType list = resolveDataType(column.getType().getTypeArguments().get(0));
                listEntityModel.addList(column.getName()).ofSimpleField(list);
                break;
            case MAP:
                listEntityModel.addDynamicObjectField(column.getName()).endDynamicObject();
                break;
            case UNKNOWN:
                listEntityModel.addPojoField(column.getName(), Object.class);
                break;
            default:
                listEntityModel.addSimpleField(column.getName(), columnDataType);
        }
    }


    protected org.mule.common.metadata.datatype.DataType resolveDataType(com.datastax.driver.core.DataType dataType) {
        switch(dataType.getName()){
            case UUID:
            case TIMEUUID:
            case TEXT:
            case VARCHAR:
            case ASCII:
            case INET:
                return STRING;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case BOOLEAN:
                return BOOLEAN;
            case INT:
            case VARINT:
            case SMALLINT:
            case TINYINT:
            case BIGINT:
                return INTEGER;
            case DECIMAL:
                return DECIMAL;
            case DATE:
                return DATE;
            case TIMESTAMP:
            case TIME:
                return DATE_TIME;
            case LIST:
                return LIST;
            case MAP:
                return MAP;
            case SET: return LIST;
            default: return UNKNOWN;
        }
    }

    public CassandraDBConnector getCassandraConnector() {
        return cassandraConnector;
    }

    public void setCassandraConnector(CassandraDBConnector cassandraConnector) {
        this.cassandraConnector = cassandraConnector;
    }
}

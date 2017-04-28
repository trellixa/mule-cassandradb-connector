/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.metadata;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;
import com.mulesoft.mule.cassandradb.utils.Constants;
import org.mule.api.annotations.MetaDataRetriever;
import org.mule.api.annotations.components.MetaDataCategory;
import org.mule.common.metadata.DefaultMetaData;
import org.mule.common.metadata.MetaData;
import org.mule.common.metadata.MetaDataKey;
import org.mule.common.metadata.builder.DefaultMetaDataBuilder;
import org.mule.common.metadata.builder.DynamicObjectBuilder;

@MetaDataCategory
public class CassandraPrimaryKeyMetadataCategory extends CassandraMetadataCategory {

    /**
     * @param key the metadata key to build the info for
     * @return {@link MetaData} for the given {@link MetaDataKey key}.
     */
    @Override @MetaDataRetriever public MetaData getInputMetaData(final MetaDataKey key) {

        //extract tables metadata from database
        TableMetadata tableMetadata = getTableMetadata(key);

        //build the metadata
        if (tableMetadata != null && tableMetadata.getColumns() != null) {
            DynamicObjectBuilder entityModel = new DefaultMetaDataBuilder().createDynamicObject(tableMetadata.getName());

            DynamicObjectBuilder primaryKey = entityModel.addDynamicObjectField(Constants.PRIMARY_KEY);

            for (ColumnMetadata column : tableMetadata.getPrimaryKey()) {
                addMetadataField(primaryKey, column);
            }

            primaryKey.endDynamicObject();
            return new DefaultMetaData(entityModel.build());
        }

        return new DefaultMetaData(null);
    }

}

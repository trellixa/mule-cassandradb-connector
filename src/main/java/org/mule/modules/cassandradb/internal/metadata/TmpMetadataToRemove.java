package org.mule.modules.cassandradb.internal.metadata;


import org.mule.metadata.api.builder.BaseTypeBuilder;
import org.mule.metadata.api.model.MetadataFormat;
import org.mule.metadata.api.model.MetadataType;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.metadata.MetadataContext;
import org.mule.runtime.api.metadata.MetadataResolvingException;
import org.mule.runtime.api.metadata.resolving.OutputTypeResolver;

public class TmpMetadataToRemove implements OutputTypeResolver<String> {

    @Override
    public MetadataType getOutputType(MetadataContext context, String key) throws MetadataResolvingException, ConnectionException {
        return BaseTypeBuilder.create(MetadataFormat.JSON).anyType().build();
    }

    @Override
    public String getResolverName() {
        return this.getClass().getName();
    }

    @Override
    public String getCategoryName() {
        return "TMP_METADATA_RETRIEVER";
    }
}

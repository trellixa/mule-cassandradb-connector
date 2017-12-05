package org.mule.modules.cassandradb.internal.metadata;


import org.mule.metadata.api.builder.BaseTypeBuilder;
import org.mule.metadata.api.model.MetadataFormat;
import org.mule.metadata.api.model.MetadataType;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.metadata.MetadataContext;
import org.mule.runtime.api.metadata.MetadataKey;
import org.mule.runtime.api.metadata.MetadataResolvingException;
import org.mule.runtime.api.metadata.resolving.OutputTypeResolver;
import org.mule.runtime.api.metadata.resolving.QueryEntityResolver;

import java.util.Set;

public class TmpMetadataToRemove implements QueryEntityResolver, OutputTypeResolver<String> {

    @Override
    public String getResolverName() {
        return this.getClass().getName();
    }

    @Override
    public MetadataType getOutputType(MetadataContext metadataContext, String s) throws MetadataResolvingException, ConnectionException {
        return null;
    }

    @Override
    public Set<MetadataKey> getEntityKeys(MetadataContext metadataContext) throws MetadataResolvingException, ConnectionException {
        return null;
    }

    @Override
    public MetadataType getEntityMetadata(MetadataContext metadataContext, String s) throws MetadataResolvingException, ConnectionException {
        return null;
    }

    @Override
    public String getCategoryName() {
        return "TMP";
    }
}

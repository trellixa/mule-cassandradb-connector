package org.mule.modules.cassandradb.internal.metadata;

import org.mule.metadata.api.model.MetadataType;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.metadata.MetadataContext;
import org.mule.runtime.api.metadata.MetadataKey;
import org.mule.runtime.api.metadata.MetadataResolvingException;
import org.mule.runtime.api.metadata.resolving.InputTypeResolver;
import org.mule.runtime.api.metadata.resolving.TypeKeysResolver;

import java.util.Set;

import static org.mule.modules.cassandradb.internal.metadata.MetadataRetriever.getConfig;

public class CassandraWithFiltersMetadataCategory implements TypeKeysResolver, InputTypeResolver<String> {
    @Override
    public Set<MetadataKey> getKeys(MetadataContext context) throws MetadataResolvingException, ConnectionException {
        return new MetadataRetriever(context.getConnection(), getConfig(context)).getMetadataKeys();
    }

    @Override
    public MetadataType getInputMetadata(MetadataContext context, String key) throws MetadataResolvingException, ConnectionException {
        return new MetadataRetriever(context.getConnection(), getConfig(context)).getMetadataWithFilters(key);
    }

    @Override
    public String getResolverName() {
        return this.getClass().getName();
    }

    @Override
    public String getCategoryName() {
        return this.getClass().getName();
    }
}

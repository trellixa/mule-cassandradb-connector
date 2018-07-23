/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.connectors.cassandradb.internal.metadata;

import org.mule.metadata.api.builder.BaseTypeBuilder;
import org.mule.metadata.api.model.MetadataType;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.metadata.MetadataContext;
import org.mule.runtime.api.metadata.MetadataKey;
import org.mule.runtime.api.metadata.MetadataResolvingException;
import org.mule.runtime.api.metadata.resolving.InputTypeResolver;
import org.mule.runtime.api.metadata.resolving.OutputTypeResolver;
import org.mule.runtime.api.metadata.resolving.QueryEntityResolver;
import org.mule.runtime.api.metadata.resolving.TypeKeysResolver;

import java.util.Set;

import static org.mule.metadata.java.api.JavaTypeLoader.JAVA;

public class CassandraMetadataResolver implements QueryEntityResolver, TypeKeysResolver, InputTypeResolver<String>, OutputTypeResolver<String> {

    @Override
    public Set<MetadataKey> getKeys(MetadataContext context) throws MetadataResolvingException, ConnectionException {
        return new MetadataRetriever(context.getConnection(), MetadataRetriever.getConfig(context)).getMetadataKeys();
    }

    @Override
    public MetadataType getInputMetadata(MetadataContext context, String key) throws MetadataResolvingException, ConnectionException {
        return new MetadataRetriever(context.getConnection(), MetadataRetriever.getConfig(context)).getMetadata(key);
    }

    @Override
    public MetadataType getOutputType(MetadataContext metadataContext, String s) throws MetadataResolvingException, ConnectionException {
        BaseTypeBuilder builder = new BaseTypeBuilder(JAVA);
        return builder.anyType().build();
    }

    @Override
    public Set<MetadataKey> getEntityKeys(MetadataContext context) throws MetadataResolvingException, ConnectionException {
        return getKeys(context);
    }

    @Override
    public MetadataType getEntityMetadata(MetadataContext context, String key) throws MetadataResolvingException, ConnectionException {
        return getInputMetadata(context, key);
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

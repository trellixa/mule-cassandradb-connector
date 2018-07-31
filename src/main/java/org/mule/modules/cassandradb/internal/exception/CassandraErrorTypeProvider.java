/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.internal.exception;

import org.mule.runtime.extension.api.annotation.error.ErrorTypeProvider;
import org.mule.runtime.extension.api.error.ErrorTypeDefinition;

import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class CassandraErrorTypeProvider implements ErrorTypeProvider {

    @Override
    public Set<ErrorTypeDefinition> getErrorTypes() {
        return Stream.of(CassandraError.values()).collect(toSet());
    }
}

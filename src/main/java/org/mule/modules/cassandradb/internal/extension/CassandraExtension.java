/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is
 * published under the terms of the Commercial Free Software license V.1, a copy of which
 * has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.internal.extension;

import org.mule.modules.cassandradb.internal.config.CassandraConfig;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.runtime.api.meta.Category;
import org.mule.runtime.extension.api.annotation.Configurations;
import org.mule.runtime.extension.api.annotation.Extension;
import org.mule.runtime.extension.api.annotation.error.ErrorTypes;
import org.mule.runtime.extension.api.annotation.license.RequiresEnterpriseLicense;

import static org.mule.runtime.api.meta.Category.SELECT;

@Extension(name = "CassandraDB", category = SELECT)
@RequiresEnterpriseLicense(allowEvaluationLicense = true)
@Configurations(CassandraConfig.class)
@ErrorTypes(CassandraError.class)
public class CassandraExtension {
}

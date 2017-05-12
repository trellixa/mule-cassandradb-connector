/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.metadata;

import org.mule.common.query.DsqlQueryVisitor;

/**
 *
 */

public class CassQueryVisitor extends DsqlQueryVisitor {

    @Override
    public org.mule.common.query.expression.OperatorVisitor operatorVisitor() {
        return new CassOperatorVisitor();
    }

}

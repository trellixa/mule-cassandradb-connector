/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.internal.exception;

public class OperationNotAppliedException extends CassandraException{

    public OperationNotAppliedException(String msg) {
        super(msg);
    }

    @Override
    public CassandraError getErrorCode() {
        return CassandraError.OPERATION_NOT_APPLIED;
    }
}
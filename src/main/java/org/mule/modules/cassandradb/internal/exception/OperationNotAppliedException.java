package org.mule.modules.cassandradb.internal.exception;

public class OperationNotAppliedException extends CassandraException{

    public OperationNotAppliedException(String msg) {
        super(msg);
    }

    public CassandraError getErrorCode() {
        return CassandraError.OPERATION_NOT_APPLIED;
    }
}

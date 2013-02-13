/**
 * Mule Cassandra Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.mulesoft.mule.cassandradb.api;

import org.apache.cassandra.thrift.IndexOperator;

/**
 * A basic POJO to receive the IndexExpresion
 * 
 */
public class IndexExpresion {

	/**
	 * The column name that we will use to compare
	 */
	private String columnName;
	
	/**
	 * The Operator that will need to be applied
	 */
	private IndexOperator op;
	
	/**
	 * The reference value used to compare the columns value with the selected operator
	 */
	private String value;

	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public IndexOperator getOp() {
		return op;
	}
	public void setOp(IndexOperator op) {
		this.op = op;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}

}

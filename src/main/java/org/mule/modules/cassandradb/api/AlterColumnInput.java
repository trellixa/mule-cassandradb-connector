/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.api;


public class AlterColumnInput {

   private String column;
   private ColumnType type;

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    // FIXME: Remove toString method.
    @Override
    public String toString() {
        return "AlterColumnInput{" +
                "column='" + column + '\'' +
                ", type=" + type +
                '}';
    }
}

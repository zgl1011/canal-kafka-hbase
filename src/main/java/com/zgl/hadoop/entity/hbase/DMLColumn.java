package com.zgl.hadoop.entity.hbase;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-10-12
 * \* Time: 9:34
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class DMLColumn {
    private boolean isKey;
    private boolean isUpdate;
    private String columnName;
    private String columnValue;

    public DMLColumn(boolean isKey,boolean isUpdate,String columnName,String columnValue){
        this.isKey = isKey;
        this.isUpdate = isUpdate;
        this.columnName = columnName;
        this.columnValue = columnValue;
    }
    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean key) {
        isKey = key;
    }

    public boolean isUpdate() {
        return isUpdate;
    }

    public void setUpdate(boolean update) {
        isUpdate = update;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(String columnValue) {
        this.columnValue = columnValue;
    }
}

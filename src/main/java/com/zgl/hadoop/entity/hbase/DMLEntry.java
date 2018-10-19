package com.zgl.hadoop.entity.hbase;

import java.util.List;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-10-12
 * \* Time: 9:32
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class DMLEntry {
    private String dbName;
    private String tableName;
    private String keyWord;
    private List<DMLColumn> dmlColumns;

    public DMLEntry(String dbName,String tableName,String keyWord){
        this.dbName = dbName;
        this.tableName = tableName;
        this.keyWord = keyWord;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public List<DMLColumn> getDmlColumns() {
        return dmlColumns;
    }

    public void setDmlColumns(List<DMLColumn> dmlColumns) {
        this.dmlColumns=dmlColumns;
    }
}

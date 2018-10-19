package com.zgl.hadoop.entity.hbase;

import java.io.Serializable;
import java.util.List;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-10-11
 * \* Time: 17:58
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class DDLEntry implements Serializable {

    private String dbName;
    private String tableName;
    private String keyWord;
    private List<DDLColumn> ddlColumns;

    public DDLEntry(String dbName,String tableName,String keyWord){
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

    public List<DDLColumn> getDdlColumns() {
        return ddlColumns;
    }

    public void setDdlColumns(List<DDLColumn> ddlColumns) {
        this.ddlColumns = ddlColumns;
    }
}

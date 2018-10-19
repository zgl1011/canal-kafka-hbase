package com.zgl.hadoop.entity.hbase;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-10-11
 * \* Time: 18:02
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class DDLColumn {
    private boolean isKey;
    private String keyWord;
    private String columnName;
    private String newColumnName;

    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean key) {
        isKey = key;
    }

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getNewColumnName() {
        return newColumnName;
    }

    public void setNewColumnName(String newColumnName) {
        this.newColumnName = newColumnName;
    }
}

package com.zgl.hadoop.entity.elasticsearch;

import java.io.Serializable;
import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-11-13
 * \* Time: 15:25
 * \* To change this template use File | Settings | File Templates.
 * \* Description: 存储es里面的对象（）
 * \
 */
public class ElasticSearchBean implements Serializable {
    private String index;
    private String type;
    private String keyWord;
    private String rowKey;

    private Map<String,Object> beanMap;

    public ElasticSearchBean(String index, String type, String keyWord, String rowKey){
        this.index = index+"-"+type;
        this.type = type;
        this.keyWord = keyWord;
        this.rowKey = rowKey;
    }

    public ElasticSearchBean(){}
    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public Map<String, Object> getBeanMap() {
        return beanMap;
    }

    public void setBeanMap(Map<String, Object> beanMap) {
        this.beanMap = beanMap;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

}

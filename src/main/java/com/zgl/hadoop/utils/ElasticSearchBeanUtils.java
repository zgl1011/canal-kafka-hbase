package com.zgl.hadoop.utils;

import com.zgl.hadoop.entity.elasticsearch.ElasticSearchBean;
import com.zgl.hadoop.entity.hbase.DMLColumn;
import com.zgl.hadoop.entity.hbase.DMLEntry;

import java.util.HashMap;
import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-11-13
 * \* Time: 15:22
 * \* To change this template use File | Settings | File Templates.
 * \* Description: 为了将数据转换成json
 * \
 */
public class ElasticSearchBeanUtils {

    public static ElasticSearchBean DMLEntryToElastiSearchBean(DMLEntry dmlEntry,String rowKey){
        ElasticSearchBean elasticSearchBean = new ElasticSearchBean(dmlEntry.getDbName(),dmlEntry.getTableName(),dmlEntry.getKeyWord(),rowKey);
        Map<String,Object> beanMap = new HashMap<>();
        for(DMLColumn dmlColumn : dmlEntry.getDmlColumns()){
            beanMap.put(dmlColumn.getColumnName(),dmlColumn.getColumnValue());
        }
        elasticSearchBean.setBeanMap(beanMap);
        return elasticSearchBean;
    }
}

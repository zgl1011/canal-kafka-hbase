package com.zgl.hadoop.service.hbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.zgl.hadoop.configuration.hbase.HBaseConnectionFactory;
import com.zgl.hadoop.configuration.hbase.HBaseDao;
import com.zgl.hadoop.constant.HBaseConstant;
import com.zgl.hadoop.entity.elasticsearch.ElasticSearchBean;
import com.zgl.hadoop.entity.hbase.DDLColumn;
import com.zgl.hadoop.entity.hbase.DDLEntry;
import com.zgl.hadoop.entity.hbase.DMLColumn;
import com.zgl.hadoop.entity.hbase.DMLEntry;
import com.zgl.hadoop.service.elasticsearch.ElasticSearchBaseService;
import com.zgl.hadoop.utils.ElasticSearchBeanUtils;
import com.zgl.hadoop.utils.ParserUtil;
import com.zgl.hadoop.utils.RedisUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-10-12
 * \* Time: 9:51
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
@Service
public class HBaseWriterOfKafkaService {

    private static final Logger logger = LoggerFactory.getLogger(HBaseWriterOfKafkaService.class);

    @Autowired
    private HBaseDao hBaseDao;

    @Autowired
    private ElasticSearchBaseService elasticSearchBaseService;
    @Autowired
    private KafkaTemplate kafkaTemplate;
    @Autowired
    private RedisUtils redisUtils;

    public void writeDDL(CanalEntry.Entry canalEntry) throws InvalidProtocolBufferException {
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(canalEntry.getStoreValue());
        logger.info("DDL_SQL===="+rowChange.getSql());
        List<DDLEntry> ddlEntries  = ParserUtil.ddlParser(canalEntry.getHeader().getSchemaName(),canalEntry.getHeader().getTableName(),canalEntry.getHeader().getEventType().name(),rowChange.getSql());
        for(DDLEntry entry:ddlEntries){

            switch (entry.getKeyWord()){
                case "CREATE":
                    writeDDLCreate(entry);
                    break;
                case "ALTER":
                 //   writeDDLDelete(canalEntry);
                    break;
            }


        }

       /* for(DDLEntry ddlEntry:ddlEntries){
            logger.info(ddlEntry.getDdlColumns().toString());
        }
*/
    }

    public void writeDML(CanalEntry.Entry canalEntry) throws InvalidProtocolBufferException {
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(canalEntry.getStoreValue());
        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
        for(CanalEntry.RowData rowData:rowDataList){
            List<CanalEntry.Column> beforeColumnList = rowData.getBeforeColumnsList();
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
//            logger.info("**************Before*********************");
//            for(CanalEntry.Column column:beforeColumnList){
//                logger.info(column.getName()+":"+column.getValue());
//            }
//            logger.info("**************After*********************");
//            for(CanalEntry.Column afterColumn:afterColumnsList){
//                logger.info(afterColumn.getName()+":"+afterColumn.getValue());
//            }
            String rowKey = null;
            DMLEntry dmlEntry;

            if(rowChange.getEventType().name().equals("DELETE")){
                dmlEntry = ParserUtil.dmlParser(canalEntry.getHeader().getSchemaName(),canalEntry.getHeader().getTableName(),rowChange.getEventType().name(),beforeColumnList);
                rowKey = getRowKey(getRowKeyColumn(dmlEntry.getDmlColumns()).getColumnValue());
            }else{
                dmlEntry = ParserUtil.dmlParser(canalEntry.getHeader().getSchemaName(),canalEntry.getHeader().getTableName(),rowChange.getEventType().name(),afterColumnsList);
                rowKey = getRowKey(getRowKeyColumn(dmlEntry.getDmlColumns()).getColumnValue());
            }
            String tableName = getHBaseTableName(dmlEntry.getDbName(),dmlEntry.getTableName());
            ElasticSearchBean elasticSearchBean = ElasticSearchBeanUtils.DMLEntryToElastiSearchBean(dmlEntry,rowKey);
    //        logger.info("********rowKey*********:{}",rowKey);
            switch (dmlEntry.getKeyWord()){
                case "INSERT":
                case "UPDATE":
                    dmlPut(rowKey,dmlEntry);
                   // this.kafkaTemplate.send(new ProducerRecord("example2Batch", JSON.toJSON(elasticSearchBean).toString()));
                  //  if(redisUtils.hasKey(redisUtils.getESRedisKey(tableName)))
                        elasticSearchBaseService.saveCanalData(elasticSearchBean);
                    break;
                case "DELETE":
                    dmlDel(rowKey,dmlEntry);
                   // if(redisUtils.hasKey(redisUtils.getESRedisKey(tableName)))
                        elasticSearchBaseService.deleteData(elasticSearchBean.getIndex(),elasticSearchBean.getType(),elasticSearchBean.getRowKey());
                    break;

            }

        }

    }

    public void writeDDLCreate(DDLEntry ddlEntry){
        String tableName = getHBaseTableName(ddlEntry.getDbName(),ddlEntry.getTableName());
        List<String> columns = new ArrayList<>();
        String rowKey = null;
        for(DDLColumn ddlColumn:ddlEntry.getDdlColumns()){

            if(ddlColumn.isKey() && StringUtils.isEmpty(rowKey)){
                rowKey = ddlColumn.getColumnName();
            }
            columns.add(ddlColumn.getColumnName());
        }
        if(!columns.isEmpty()){
            try {
                if(!hBaseDao.hasNamespace(ddlEntry.getDbName()))
                    hBaseDao.createNamespace(ddlEntry.getDbName());
                hBaseDao.createTable(tableName,new ArrayList<String>(){{add(HBaseConstant.DETAULT_FAMILY);}},true);
                elasticSearchBaseService.createIndex(elasticSearchBaseService.getIndexName(ddlEntry.getDbName(),ddlEntry.getTableName()));
   //             elasticSearchBaseService.createMapping(elasticSearchBaseService.getIndexName(ddlEntry.getDbName(),ddlEntry.getTableName()),ddlEntry.getTableName(),ddlEntry.getDdlColumns());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void writeDDLDrop(CanalEntry.Entry canalEntry){
        try {
            hBaseDao.deleteTable(getHBaseTableName(canalEntry.getHeader().getSchemaName(),canalEntry.getHeader().getTableName()));
            elasticSearchBaseService.deleteIndex(elasticSearchBaseService.getIndexName(canalEntry.getHeader().getSchemaName(),canalEntry.getHeader().getTableName()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeDDLAlter(DDLEntry ddlEntry){
        List<String> addColumnList = new ArrayList<>();
        List<String> delColumnList = new ArrayList<>();
        String tableName = getHBaseTableName(ddlEntry.getDbName(),ddlEntry.getTableName());
        try {
            for(DDLColumn ddlColumn:ddlEntry.getDdlColumns()) {
                switch (ddlColumn.getKeyWord()) {
                    case "DROP":
                        //delColumnList.add(ddlColumn.getColumnName());
                        hBaseDao.deleteTable(tableName);
                        return;
                    case "ADD":
                        addColumnList.add(ddlColumn.getColumnName());
                        break;
                    case "CHANGE":
                        logger.warn("change col name is not support, table:{}, col:{}, changeCol:{}.",
                                tableName, ddlColumn.getColumnName(), ddlColumn.getNewColumnName());
                        break;
                    default:
                }
            }

            if (!addColumnList.isEmpty()) {
                hBaseDao.addColumn(tableName, addColumnList);
            }
            if (!delColumnList.isEmpty()) {
                hBaseDao.delColumn(tableName, delColumnList);
            }
        } catch (Exception e) {
            logger.warn("alter table failed. msg:{}", e.getMessage());
        }

    }
    private String getHBaseTableName(String schemaName, String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append(schemaName);
        sb.append(":");
        sb.append(tableName);

        tableName = sb.toString();
        return tableName;
    }

    public DMLColumn getRowKeyColumn(List<DMLColumn> dmlCols) {
        DMLColumn dmlCol = null;

        for (DMLColumn col : dmlCols) {
            if (col.isKey()) {
                dmlCol = col;
                break;
            }
        }
        return dmlCol;
    }

    public void dmlPut(String rowKey,DMLEntry dmlEntry){
        List<Put> puts = getPuts(rowKey,dmlEntry.getDmlColumns());
        String tableName = getHBaseTableName(dmlEntry.getDbName(),dmlEntry.getTableName());
        if(puts!=null && !puts.isEmpty()) {
            try {
                hBaseDao.putByHTable(tableName,puts);
         //       hBaseDao.put(tableName,puts);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public void dmlDel(String rowKey,DMLEntry dmlEntry){
        String tableName = getHBaseTableName(dmlEntry.getDbName(),dmlEntry.getTableName());
        hBaseDao.delete(tableName,rowKey);

    }
    public List<Put> getPuts(String rowKey, List<DMLColumn> columns){
        List<Put> puts = new ArrayList<>();
        for(DMLColumn column:columns){
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(HBaseConstant.DETAULT_FAMILY),
                    Bytes.toBytes(column.getColumnName() == null ? HBaseConstant.DEFAULT_QUALIFIER : column.getColumnName()),
//                    Bytes.toBytes(""),
                    Bytes.toBytes(column.getColumnValue() == null ? "" : column.getColumnValue()));
            puts.add(put);
        }
        return puts;
    }

    public List<Put> getPuts(DMLEntry dmlEntry){
        String rowKey = getRowKey(getRowKeyColumn(dmlEntry.getDmlColumns()).getColumnValue());
        return getPuts(rowKey,dmlEntry.getDmlColumns());
    }

    private String getRowKey(String rowKey){
        StringBuffer rowKeyBuf = new StringBuffer();
        String[] splits = HBaseConnectionFactory.splits;
        //根据rowKey的hashCode对splits的大小取余获取splits的下标，注意hashCode有可能为负值所以需要转化
        String slat = splits[(rowKey.hashCode()& 0x7fffffff) % splits.length];
        if(!StringUtils.isEmpty(slat)){
            return rowKeyBuf.append(slat).append("_").append(rowKey).toString();    //通过加盐的值的形式对region进行切分
        }else {
            return rowKey;
        }
    }


    public void writeDMLAsync(CanalEntry.Entry canalEntry) throws InvalidProtocolBufferException {
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(canalEntry.getStoreValue());
        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
        for(CanalEntry.RowData rowData:rowDataList){
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            String rowKey = null;
            DMLEntry dmlEntry;
            dmlEntry = ParserUtil.dmlParser(canalEntry.getHeader().getSchemaName(),canalEntry.getHeader().getTableName(),rowChange.getEventType().name(),afterColumnsList);
            String tableName = getHBaseTableName(dmlEntry.getDbName(),dmlEntry.getTableName());
            rowKey = getRowKey(getRowKeyColumn(dmlEntry.getDmlColumns()).getColumnValue());
            ElasticSearchBean elasticSearchBean = ElasticSearchBeanUtils.DMLEntryToElastiSearchBean(dmlEntry,rowKey);
         //   if(redisUtils.hasKey(redisUtils.getESRedisKey(tableName)))
                this.kafkaTemplate.send(new ProducerRecord("BatchESTopic", JSON.toJSON(elasticSearchBean).toString()));
           // List<Put> puts = getPuts(rowKey,dmlEntry.getDmlColumns());

            this.kafkaTemplate.send(new ProducerRecord(dmlEntry.getTableName(), JSON.toJSON(dmlEntry).toString()));
        }

    }

    public void dmlBatchPut(List<Put> puts,String tableName){
        try {

            String dbName = tableName.split(":")[0];
            if(!hBaseDao.tableExists(tableName)){
                if(!hBaseDao.hasNamespace(dbName)) {
                    hBaseDao.createNamespace(dbName);
                }
                hBaseDao.createTable(tableName,new ArrayList<String>(){{add(HBaseConstant.DETAULT_FAMILY);}},true);
            }
            if(puts!=null && !puts.isEmpty()) {
                try {
                    hBaseDao.putByHTable(tableName,puts);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package com.zgl.hadoop.configuration.kafka;


import com.alibaba.fastjson.JSON;
import com.zgl.hadoop.constant.HBaseConstant;
import com.zgl.hadoop.entity.elasticsearch.ElasticSearchBean;
import com.zgl.hadoop.entity.hbase.DMLEntry;
import com.zgl.hadoop.service.elasticsearch.ElasticSearchBaseService;
import com.zgl.hadoop.service.hbase.HBaseWriterOfKafkaService;
import org.apache.hadoop.hbase.client.Put;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: zgl
 * @Descriptions:批量消费
 * @Date: Created in 2018/3/23
 */
@Component("listenerBatch")
public class ListenerBatch {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private ElasticSearchBaseService elasticSearchBaseService;
    @Autowired
    private HBaseWriterOfKafkaService hBaseWriterOfKafkaService;

    //批量入ES
    @KafkaListener(topics = {"BatchESTopic"},containerFactory = "kafkaListenerContainerFactoryBatch")
    public void listenBatchES(List<ConsumerRecord<String, String>> records) {
        logger.info("es batch :"+records.size());
        List<ElasticSearchBean> esList = new ArrayList<>();
        records.forEach(record -> {
//            logger.info("收到的数据"+record.value());
            esList.add(JSON.parseObject(record.value(),  ElasticSearchBean.class));
        });
        elasticSearchBaseService.saveDataByBulk(esList);
    }

    //批量入HBase
    @KafkaListener(topics = {"historyalarm_ar"},containerFactory = "kafkaListenerContainerFactoryBatch")
    public void listenBatchHBase(List<ConsumerRecord<String, String>> records) {
        logger.info("hbase batch :"+records.size());
        List<Put> puts = new ArrayList<>();
        records.forEach(record -> {
//            logger.info("收到的数据"+record.value());
            DMLEntry dmlEntry = JSON.parseObject(record.value(),DMLEntry.class);

            puts.addAll(hBaseWriterOfKafkaService.getPuts(dmlEntry));
        });
        hBaseWriterOfKafkaService.dmlBatchPut(puts,HBaseConstant.ALARM_TABLE_NAME);
    }
    @KafkaListener(topics = {"t_order"},containerFactory = "kafkaListenerContainerFactoryBatch")
    public void listenBatchHBaseOrder(List<ConsumerRecord<String, String>> records) {
        logger.info("hbase batch :"+records.size());
        List<Put> puts = new ArrayList<>();
        records.forEach(record -> {
//            logger.info("收到的数据"+record.value());
            DMLEntry dmlEntry = JSON.parseObject(record.value(),DMLEntry.class);

            puts.addAll(hBaseWriterOfKafkaService.getPuts(dmlEntry));
        });
        hBaseWriterOfKafkaService.dmlBatchPut(puts,HBaseConstant.ORDER_TABLE_NAME);
    }
}

package com.zgl.hadoop.configuration.kafka;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.zgl.hadoop.service.hbase.HBaseWriterOfKafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


/**
 * @Author: zgl
 * @Descriptions:
 * @Date: Created in 2018/3/23
 */
@Component("listener")
public class Listener {

    @Autowired
    private HBaseWriterOfKafkaService hBaseWriterOfKafka;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 资产数据
     * @param record
     */
    @KafkaListener(topics = {"oms_newStake"})
    public void listenOmsNewStake(ConsumerRecord<String, Message> record) {
        try {
            Message message = record.value();
            transactionalIncrement(message);

        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }

    }

    /**
     * 监控数据
     * @param record
     */
    @KafkaListener(topics = {"oms_monitor"})
    public void listenOmsMonitor(ConsumerRecord<String, Message> record) {
        try {
            Message message = record.value();
            transactionalIncrement(message);

        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    /**
     * 工单数据
     * @param record
     */
    @KafkaListener(topics = {"iov_model"})
    public void listenIovModel(ConsumerRecord<String, Message> record) {
        try {
//            logger.info("TOrderConsumer--->gemini.t_order～～～～～～listen");
//            logger.info("---|offset = %d,topic= %s,partition=%s,key =%s,value=%s\n", record.offset(), record.topic(), record.partition(), record.key(), record.value());
//            logger.info("sss---------:" + record.value().toString());

            Message message = record.value();

            transactionalIncrement(message);

        } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
        }

    }

    /**
     * 历史告警数据
     * @param record
     *
     * 异步操作，仅支持增量数据不带有事物的
     */

    @KafkaListener(topics = {"oms_historyalarm"})
    public void listenOmsHistoryAlarmAsync(ConsumerRecord<String, Message> record) {
        try {
            Message message = record.value();
            increment(message);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }


    /**
     * 订单数据
     * @param record
     *
     * 异步操作，仅支持增量数据不带有事物的
     */

    @KafkaListener(topics = {"oms_order"})
    public void listenOmsOrderAsync(ConsumerRecord<String, Message> record) {
        try {
            Message message = record.value();
            increment(message);

        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    /**
     * 带有事物操作的增量
     * @param message
     * @throws InvalidProtocolBufferException
     */
    public void transactionalIncrement(Message message) throws InvalidProtocolBufferException {
        if (!message.getEntries().isEmpty()) {
            for (CanalEntry.Entry canalEntry : message.getEntries()) {

                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(canalEntry.getStoreValue());
//                    logger.info("storeValue====" + new String(rowChange.toString().getBytes(), "UTF-8"));
                switch (rowChange.getEventType()) {
                    case CREATE:
                        this.hBaseWriterOfKafka.writeDDL(canalEntry);
                        break;
                    case ERASE:         //drop语句仅支持单个表的操作
                        this.hBaseWriterOfKafka.writeDDLDrop(canalEntry);
                        break;
                    case INSERT:
                        this.hBaseWriterOfKafka.writeDML(canalEntry);
                        break;
                    case UPDATE:
                        this.hBaseWriterOfKafka.writeDML(canalEntry);
                        break;
                    case DELETE:
                        this.hBaseWriterOfKafka.writeDML(canalEntry);
                        break;
                }
            }
        }
    }

    /**
     * 增量，不带有事物操作
     * @param message
     * @throws InvalidProtocolBufferException
     */
    public void increment(Message message) throws InvalidProtocolBufferException {
        if (!message.getEntries().isEmpty()) {
            for (CanalEntry.Entry canalEntry : message.getEntries()) {

                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(canalEntry.getStoreValue());
                switch (rowChange.getEventType()) {
                    case CREATE:
                        this.hBaseWriterOfKafka.writeDDL(canalEntry);
                        break;
                    case ERASE:         //drop语句仅支持单个表的操作
                        this.hBaseWriterOfKafka.writeDDLDrop(canalEntry);
                        break;
                    case INSERT:
                        this.hBaseWriterOfKafka.writeDMLAsync(canalEntry);
                        break;
                }
            }
        }
    }

}

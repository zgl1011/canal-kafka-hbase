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

import java.io.UnsupportedEncodingException;

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

   // @KafkaListener(topics = {"notice"})
    public void listen(ConsumerRecord<?, ?> record) {
        logger.info("kafka的key: " + record.key());
        logger.info("kafka的value: " + record.value().toString());

    }

   // @KafkaListener(topics = {"notice2"})
    public void listen2(ConsumerRecord<?, ?> record) {
        logger.info("kafka2的key: " + record.key());
        logger.info("kafka2的value: " + record.value().toString());
    }

    @KafkaListener(topics = {"example"})
    public void listenCanal(ConsumerRecord<String, Message> record) {
        try {
//            logger.info("TOrderConsumer--->gemini.t_order～～～～～～listen");
//            logger.info("---|offset = %d,topic= %s,partition=%s,key =%s,value=%s\n", record.offset(), record.topic(), record.partition(), record.key(), record.value());
            logger.info("sss---------:" + record.value().toString());

            Message message = record.value();
//            List<FlatMessage> flatMessages = FlatMessage.messageConverter(message);
//            for(FlatMessage flatMessage:flatMessages){
//                logger.info(flatMessage.toString());
//            }
            if (!message.getEntries().isEmpty()) {
                for (CanalEntry.Entry canalEntry : message.getEntries()){
                        //               CanalEntry.Entry canalEntry = message.getEntries().get(0);
    //                CanalEntry.Column column = CanalEntry.Column.parseFrom(canalEntry.getStoreValue());
    //                logger.info("CanalEntry.Column=="+column.toString());
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(canalEntry.getStoreValue());
                    logger.info("storeValue====" + new String(rowChange.toString().getBytes(), "UTF-8"));
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

        } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


        //  Message message = CanalMessageDeserializer.deserializer(record.value().toString().getBytes());
       /* CanalBean canalBean = JSON.parseObject(record.value().toString(), CanalBean.class);
        switch(canalBean.getEventType()){
            case CanalEntry.EventType.INSERT_VALUE:
                logger.info("canalBean.getEventType():"+canalBean.getEventType());
                logger.info("CanalEntry.EventType.INSERT_VALUE:"+CanalEntry.EventType.INSERT_VALUE);
                //20服务器上的gemini库的t_order平行同步insert的数据到Hbase的表gemini.t_order
                insert(canalBean);
                break;
            case CanalEntry.EventType.UPDATE_VALUE:
                update(canalBean);
                break;
            case CanalEntry.EventType.DELETE_VALUE:
                delete(canalBean);
                break;
            case CanalEntry.EventType.CREATE_VALUE:
                //TODO
                break;
            case CanalEntry.EventType.ALTER_VALUE:
                //TODO
                break;
            case CanalEntry.EventType.ERASE_VALUE:
                //TODO
                break;
            case CanalEntry.EventType.QUERY_VALUE:
                //TODO
                break;

        }*/
    }
}

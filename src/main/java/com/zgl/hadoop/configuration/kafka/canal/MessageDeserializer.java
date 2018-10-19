package com.zgl.hadoop.configuration.kafka.canal;

import com.alibaba.otter.canal.protocol.Message;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-10-10
 * \* Time: 11:45
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class MessageDeserializer  implements Deserializer<Message> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Message deserialize(String topic1, byte[] data) {
        return CanalMessageDeserializer.deserializer(data);
    }

    @Override
    public void close() {
        // nothing to do
    }
}

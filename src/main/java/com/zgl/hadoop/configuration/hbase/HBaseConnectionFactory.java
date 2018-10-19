package com.zgl.hadoop.configuration.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @Author: zgl
 * @Descriptions: 加载Hbase连接
 * @Date: Created in 2018/3/21
 */
@Component
public class HBaseConnectionFactory implements InitializingBean {

    protected final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${hbase.zookeeper.quorum}")
    private String zkQuorum;

    @Value("${hbase.master}")
    private String hBaseMaster;

    @Value("${hbase.zookeeper.property.clientPort}")
    private String zkPort;

    @Value("${zookeeper.znode.parent}")
    private String znode;

    @Value("${hbase.split.info}")
    private String splitInfo;

    private static Configuration conf = HBaseConfiguration.create();

    public static Connection connection;

    public static String[] splits;

    @Override
    public void afterPropertiesSet(){
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("hbase.zookeeper.property.clientPort", zkPort);
        conf.set("zookeeper.znode.parent", znode);
        conf.set("hbase.master", hBaseMaster);
        try {
            connection = ConnectionFactory.createConnection(conf);
            logger.info("获取connectiont连接成功！");
        } catch (IOException e) {
            e.printStackTrace ();
            logger.error("获取connectiont连接失败！");
        }
        splits = splitInfo.split("\\,");
    }
}

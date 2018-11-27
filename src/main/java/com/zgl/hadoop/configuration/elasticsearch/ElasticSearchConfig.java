package com.zgl.hadoop.configuration.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-11-21
 * \* Time: 11:08
 * \* To change this template use File | Settings | File Templates.
 * \* Description: 数据通过TransportClient来连接集群，完成对es集群的数据操作
 * \
 */
@Configuration
public class ElasticSearchConfig {

    @Value("${elasticsearch.cluster.nodes}")
    private String clusterNodes;

    @Value("${elasticsearch.cluster.name}")
    private String clusterName;


    @Bean
    public Client client(){
        Settings settings = Settings.builder().put("cluster.name", clusterName)
                .put("client.transport.sniff", true).build();

        TransportClient client = new PreBuiltTransportClient(settings);
        try {
            if (clusterNodes != null && !"".equals(clusterNodes)) {
                for (String node : clusterNodes.split(",")) {
                    String[] nodeInfo = node.split(":");
                    client.addTransportAddress(new TransportAddress(InetAddress.getByName(nodeInfo[0]), Integer.parseInt(nodeInfo[1])));
                }
            }
        } catch (UnknownHostException e) {
        }

        return client;
    }
}

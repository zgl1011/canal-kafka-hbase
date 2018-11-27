package com.zgl.hadoop.service.elasticsearch;

import com.zgl.hadoop.constant.ElasticSearchConstant;
import com.zgl.hadoop.entity.elasticsearch.ElasticSearchBean;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-11-21
 * \* Time: 14:34
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
@Service
public class ElasticSearchBaseService {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchBaseService.class);

    @Autowired
    private Client client;


    /**
     * 判断索引存不存在
     * @param index
     * @return
     */
    public boolean indexIsExists(String index){
        IndicesExistsResponse indicesExistsResponse = client.admin().indices().prepareExists(index).get();

        return indicesExistsResponse.isExists();

    }

    /**
     * 创建索引
     */
    public void createIndex(String index,int shards,int replicas){

        CreateIndexResponse response = client.admin().indices().prepareCreate(index)
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_replicas", replicas))
                .get();

    }

    /**
     * 插入Map类型的数据
     * @param index
     * @param type
     * @param id
     * @param data
     * @return
     */
   // @Async("myExecutor")
    public IndexResponse saveData(String index, String type, String id, Map<String,Object> data){
        return client.prepareIndex(index, type, id)
                .setSource(data).get();
    }


    //批量bulk插入数据
    @Async("myExecutor")
    public void saveDataByBulk(List<ElasticSearchBean> list){
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        // 添加index操作到 bulk 中
        list.forEach(esBean -> {
            bulkRequestBuilder.add(client.prepareIndex(esBean.getIndex(), esBean.getType(), esBean.getRowKey()).setSource(esBean.getBeanMap()));
        });

        BulkResponse responses = bulkRequestBuilder.execute().actionGet();
        if (responses.hasFailures()) {
            // bulk有失败
            for (BulkItemResponse res : responses) {
                LOG.error("es bulk error:"+res.getFailure());
            }
        }
        LOG.info("bulk write 【"+list.size()+"】 item");
    }

    public void deleteData(String index,String type,String id){
         client.prepareDelete(index, type, id).execute().actionGet();
    }

    //异步写入
    @Async("myExecutor")
    public void saveCanalData(ElasticSearchBean elasticSearchBean){
        if(!indexIsExists(elasticSearchBean.getIndex()))
            createIndex(elasticSearchBean.getIndex(),ElasticSearchConstant.DEFAULT_SHARDS,ElasticSearchConstant.DEFAULT_REPLICAS);

        IndexResponse indexResponse = saveData(elasticSearchBean.getIndex(),elasticSearchBean.getType(),elasticSearchBean.getRowKey(),elasticSearchBean.getBeanMap());

        LOG.info("数据写入结果："+indexResponse.getId());
    }


}

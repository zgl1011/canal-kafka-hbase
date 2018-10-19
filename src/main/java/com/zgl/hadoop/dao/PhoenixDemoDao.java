package com.zgl.hadoop.dao;

import com.zgl.hadoop.utils.ResultData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-8-21
 * \* Time: 11:28
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
@Component
public class PhoenixDemoDao {

    @Autowired
    @Qualifier("phoenixJdbcTemplate")
    JdbcTemplate phoenixJdbcTemplate;
    public ResultData add(){

        phoenixJdbcTemplate.update("upsert into company(id,name,address) values('20','xuxiao','德国柏林')");

        return new ResultData(true,"数据添加成功");

    }

    public ResultData update(){
        int res = phoenixJdbcTemplate.update("upsert into company(id,name) values('20','yyggg')");
        return new ResultData(true,"数据更新成功");

    }

    public ResultData delete(){
        phoenixJdbcTemplate.update("delete from company where id ='20'");
        return new ResultData(true,"数据删除成功");

    }


    public List<Map<String, Object>> query(){
        return phoenixJdbcTemplate.queryForList("select * from test.person");

    }
}

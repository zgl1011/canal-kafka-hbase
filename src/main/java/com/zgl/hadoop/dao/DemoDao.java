package com.zgl.hadoop.dao;

import com.zgl.hadoop.entity.Demo;
import com.zgl.hadoop.configuration.hbase.HBaseBeanDaoUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @Author: zgl
 * @Descriptions:
 * @Date: Created in 2018/3/21
 */
@Component("demoDao")
public class DemoDao {

    @Autowired
    private HBaseBeanDaoUtil hBaseDaoUtil;

    /**
     * @Descripton:
     * @Author: zgl
     * @param demo
     * @Date: 2018/3/22
     */
    public void save(Demo demo) {
        hBaseDaoUtil.save(demo);
    }

    /**
     * @Descripton:
     * @Author: zgl
     * @param demo
     * @param id
     * @Date: 2018/3/22
     */
    public List<Demo> getById(Demo demo, String id) {
        return hBaseDaoUtil.get(demo, id);
    }
}

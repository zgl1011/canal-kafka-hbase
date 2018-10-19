package com.zgl.hadoop.service;

import com.zgl.hadoop.dao.DemoDao;
import com.zgl.hadoop.entity.Demo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * @Author: zgl
 * @Descriptions:
 * @Date: Created in 2018/3/21
 */
@Service("demoService")
@Transactional(readOnly = true)
public class DemoService {

    @Autowired
    private DemoDao demoDao;

    /**
     * @Descripton:
     * @Author: zgl
     * @param demo
     * @Date: 2018/3/22
     */
    @Transactional(readOnly = false, rollbackFor = Exception.class)
    public void save(Demo demo) {
        demoDao.save(demo);
    }

    /**
     * @Descripton:
     * @Author: zgl
     * @param demo
     * @param id
     * @Date: 2018/3/22
     */
    @Transactional
    public List<Demo> getById(Demo demo, String id) {
        return demoDao.getById(demo, id);
    }
}

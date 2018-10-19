package com.zgl.hadoop.service;

import com.zgl.hadoop.dao.PhoenixDemoDao;
import com.zgl.hadoop.utils.ResultData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-8-21
 * \* Time: 11:27
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
@Service
public class PhoenixDemoService {
    @Autowired
    PhoenixDemoDao phoenixDemoDao;

    public ResultData add(){
        return this.phoenixDemoDao.add();
    }
    public ResultData update(){
        return this.phoenixDemoDao.update();
    }
    public ResultData delete(){
        return this.phoenixDemoDao.delete();
    }
    public ResultData query(){
        ResultData resultData = new ResultData();
        resultData.setData(this.phoenixDemoDao.query());
        resultData.setSuccess(true);
        return resultData;
    }
}

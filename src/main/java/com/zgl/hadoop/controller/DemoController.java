package com.zgl.hadoop.controller;

import com.google.common.collect.ImmutableMap;
import com.zgl.hadoop.entity.Demo;
import com.zgl.hadoop.service.DemoService;
import com.zgl.hadoop.utils.BaseController;
import com.zgl.hadoop.utils.ResultData;
import com.zgl.hadoop.utils.ResultType;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @Author: zgl
 * @Descriptions:
 * @Date: Created in 2018/3/21
 */
@RestController
@RequestMapping("/demo")
public class DemoController extends BaseController {

    @Autowired
    private DemoService demoService;

    @ApiOperation(value="测试controller", notes="")
    @GetMapping("/test")
    public String test(){
        return "test";
    }

    /**
     * @Descripton: 保存
     * @Author: zgl
     * @param demo
     * @Date: 2018/3/22
     */
    @ApiOperation(value="保存数据接口", notes="保存数据接口")
    @PostMapping("/save")
    public ResultData save(Demo demo){
        try {
            demoService.save(demo);
            return setResponseEntity(ResultType.SUCCESS.getDescription(), ResultType.SUCCESS.getCode(),null,true);
        } catch (Exception e) {
            e.printStackTrace();
            return setResponseEntity(ResultType.SERVER_ERROR.getDescription(),ResultType.SERVER_ERROR.getCode(),null,false);
        }
    }

    /**
     * @Descripton: 根据id查询
     * @Author: zgl
     * @param id
     * @Date: 2018/3/22
     */
    @ApiOperation(value="根据id查询数据接口", notes="根据id查询数据接口")
    @GetMapping("/get/{id}")
    public ResultData getBean(@PathVariable String id){
        try {
            List<Demo> apples = demoService.getById(new Demo(), id);
            return setResponseEntity(ResultType.SUCCESS.getDescription(), ResultType.SUCCESS.getCode(), ImmutableMap.of("date", apples),true);
        } catch (Exception e) {
            e.printStackTrace();
            return setResponseEntity(ResultType.SERVER_ERROR.getDescription(),ResultType.SERVER_ERROR.getCode(),null,false);
        }
    }



}

package com.zgl.hadoop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;

/**
 * @Author: zgl
 * @Descriptions:
 * @Date: Created in 2018/3/21
 */
@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class} )
public class HadoopStartApp {

    public static void main(String[] args) {
        SpringApplication.run(HadoopStartApp.class, args);
    }
}

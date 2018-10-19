package com.zgl.hadoop.configuration.phoenix;

import com.alibaba.druid.pool.DruidDataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-8-21
 * \* Time: 10:12
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
@Configuration
public class PhoenixDataSource {
    @Autowired
    private Environment env;

    @Bean(name = "phoenixJdbcDataSource")
    @Qualifier("phoenixJdbcDataSource")
    public DataSource dataSource() {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(env.getProperty("phoenix.url"));
        dataSource.setDriverClassName(env.getProperty("phoenix.driver-class-name"));
        dataSource.setUsername(env.getProperty("phoenix.username"));//phoenix的用户名默认为空
        dataSource.setPassword(env.getProperty("phoenix.password"));//phoenix的密码默认为空
        dataSource.setDefaultAutoCommit(Boolean.valueOf(env.getProperty("phoenix.default-auto-commit")));
        dataSource.setValidationQuery(env.getProperty("phoenix.validationQuery"));
        return dataSource;
    }

    @Bean(name = "phoenixJdbcTemplate")
    public JdbcTemplate phoenixJdbcTemplate(@Qualifier("phoenixJdbcDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}

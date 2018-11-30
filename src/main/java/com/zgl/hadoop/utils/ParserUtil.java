package com.zgl.hadoop.utils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;

import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;


import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.zgl.hadoop.constant.KeyWordConstant;
import com.zgl.hadoop.entity.hbase.DDLColumn;
import com.zgl.hadoop.entity.hbase.DDLEntry;
import com.zgl.hadoop.entity.hbase.DMLColumn;
import com.zgl.hadoop.entity.hbase.DMLEntry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-10-11
 * \* Time: 17:48
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class ParserUtil {
    private static final Logger log = LoggerFactory.getLogger(ParserUtil.class);
    private static final String REGEX = "[`'\"]";


    public static boolean equalMultipleStrings(String str, List<String> list) {
        for (String s : list) {
            if (StringUtils.equalsIgnoreCase(str, s)) {
                return true;
            }
        }
        return false;
    }

    public static DMLEntry dmlParser(final String schemaName,
                                        final String tableName,
                                        final String keyWord,
                                        List<CanalEntry.Column> columns) {
        DMLEntry dmlEntry = new DMLEntry(schemaName,tableName,keyWord);


        // check key counts, 不允许有多个key的情况.
        List<DMLColumn> dmlCols = new ArrayList<>();
        for (CanalEntry.Column column : columns) {
            DMLColumn dmlCol = new DMLColumn(column.getIsKey(),column.getUpdated(),column.getName(),column.getValue());

            dmlCols.add(dmlCol);
        }

        dmlEntry.setDmlColumns(dmlCols);
        return dmlEntry;
    }

    public static List<DDLEntry> ddlParser(final String schemaName,
                                                 final String tableName,
                                                 final String keyWord,
                                                 final String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        List<DDLEntry> ddlEntries = new ArrayList<>();

        for (SQLStatement sqlStatement : statementList) {
            DDLEntry ddlEntry = new DDLEntry(schemaName,tableName,keyWord);

            List<DDLColumn> ddlColumns = new ArrayList<>();

            // 0.1:仅考虑alter和create操作
            if (sqlStatement instanceof SQLAlterTableStatement) {
                SQLAlterTableStatement stmt = (SQLAlterTableStatement) sqlStatement;
                List<SQLAlterTableItem> items = stmt.getItems();
                for (SQLAlterTableItem item : items) {
                    DDLColumn ddlColumn = new DDLColumn();

                    if (item instanceof SQLAlterTableAddColumn) {
                        SQLAlterTableAddColumn stmtOfAddCol = (SQLAlterTableAddColumn) item;
                        // mysql语法:每一条add column stmt 一个col
                        for (SQLColumnDefinition column : stmtOfAddCol.getColumns()) {
                            String columnName = replaceAll(column.getName().getSimpleName());
                            ddlColumn.setKeyWord(KeyWordConstant.ADD);
                            ddlColumn.setNewColumnName(columnName);
                        }
                    }

                    if (item instanceof MySqlAlterTableChangeColumn) {
                        MySqlAlterTableChangeColumn stmtOfChangeCol = (MySqlAlterTableChangeColumn) item;
                        String originColName = replaceAll(stmtOfChangeCol.getColumnName().getSimpleName());
                        String newColName = replaceAll(stmtOfChangeCol.getNewColumnDefinition().getName().getSimpleName());
                        // if originColName == newColName
                        if (StringUtils.equals(originColName, newColName)) {
                            continue;
                        }
                        ddlColumn.setKeyWord(KeyWordConstant.CHANGE);
                        ddlColumn.setNewColumnName(newColName);
                        ddlColumn.setColumnName(originColName);
                    }

                    if (item instanceof SQLAlterTableDropColumnItem) {
                        SQLAlterTableDropColumnItem stmtOfDropCol = (SQLAlterTableDropColumnItem) item;
                        // mysql语法:每一条drop column stmt 一个col
                        for (SQLName col : stmtOfDropCol.getColumns()) {
                            String colName = replaceAll(col.getSimpleName());
                            ddlColumn.setKeyWord(KeyWordConstant.DROP);
                            ddlColumn.setColumnName(colName);
                        }
                    }

                        ddlColumns.add(ddlColumn);
                }
            }

            if (sqlStatement instanceof MySqlCreateTableStatement) {
                MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) sqlStatement;
                int tableElementSize = stmt.getTableElementList().size();

                for (SQLTableElement element : stmt.getTableElementList()) {
                    DDLColumn ddlColumn = new DDLColumn();

                    if (element instanceof SQLColumnDefinition) {
                        String colName = ((SQLColumnDefinition) element).getName().getSimpleName();
                        ddlColumn.setKeyWord(KeyWordConstant.CREATE);
                        ddlColumn.setColumnName(replaceAll(colName));

                        if (tableElementSize == 1) {
                            ddlColumn.setKey(true);
                        }
                    }

                    if (element instanceof MySqlPrimaryKey) {
                        List<SQLSelectOrderByItem> exprList = ((MySqlPrimaryKey)element).getColumns();
                        for (SQLSelectOrderByItem expr : exprList) {

                                SQLExpr sqlExpr = (SQLIdentifierExpr)expr.getExpr();

                                String name = ((SQLIdentifierExpr) sqlExpr).getName();
                                ddlColumn.setKeyWord(KeyWordConstant.CREATE);
                                ddlColumn.setColumnName(replaceAll(name));
                                ddlColumn.setKey(true);

                                // 移除已有的数据,重新添加带key的数据
                                int size = ddlColumns.size();
                                for (int i = 0; i < size; i++) {
                                    if (StringUtils.equalsIgnoreCase(replaceAll(name), ddlColumns.get(0).getColumnName())) {
                                        ddlColumns.remove(ddlColumns.get(0));
                                    }
                                }
                        }
                    }


                    ddlColumns.add(ddlColumn);

                }
            }
           /* if(sqlStatement instanceof SQLDropTableStatement){
                SQLDropTableStatement sqlDropTableStatement = (SQLDropTableStatement)sqlStatement;
                sqlDropTableStatement.
                MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
                sqlStatement.accept(visitor);
                visitor.getTables().get(0).
            }*/
            if (!ddlColumns.isEmpty()) {
                ddlEntry.setDdlColumns(ddlColumns);
                ddlEntries.add(ddlEntry);
            }
        }

        return ddlEntries;
    }

    public static String replaceAll(String s) {
        Pattern pattern = Pattern.compile(REGEX);
        Matcher matcher = pattern.matcher(s);
        String number = matcher.replaceAll("");
        return number;
    }

    public static void main(String args[]){
        String sql = "CREATE TABLE `rtm_alarm` (\n" +
                "  `uuid` varchar(40) NOT NULL COMMENT '主键',\n" +
                "  `province_id` varchar(16) DEFAULT NULL COMMENT '行政单位_省份ID',\n" +
                "  `province_name` varchar(64) DEFAULT NULL COMMENT '行政单位_省份名称',\n" +
                "  `city_id` varchar(16) DEFAULT NULL COMMENT '行政单位_城市ID',\n" +
                "  `city_name` varchar(64) DEFAULT NULL COMMENT '行政单位_城市名称',\n" +
                "  `county_id` varchar(16) DEFAULT NULL COMMENT '行政单位_地区ID',\n" +
                "  `county_name` varchar(64) DEFAULT NULL COMMENT '行政单位_地区名称',\n" +
                "  `province_company` varchar(16) DEFAULT NULL COMMENT '运维单位_省份编码',\n" +
                "  `city_company` varchar(16) DEFAULT NULL COMMENT '运维单位_城市编码',\n" +
                "  `county_company` varchar(16) DEFAULT NULL COMMENT '运维单位_班组编码',\n" +
                "  `station_no` varchar(32) DEFAULT NULL COMMENT '站编码',\n" +
                "  `station_name` varchar(64) DEFAULT NULL COMMENT '站名称',\n" +
                "  `is_affirm` varchar(2) DEFAULT NULL COMMENT '是否确认， 暂时没用 0 未确认 1 确认',\n" +
                "  `stake_no` varchar(32) DEFAULT NULL COMMENT '桩编号',\n" +
                "  `stake_name` varchar(255) DEFAULT NULL COMMENT '桩名称',\n" +
                "  `charging_type` varchar(2) DEFAULT NULL COMMENT '直流or交流 AC 交流 DC 直流 AD 交直流一体',\n" +
                "  `fault_type` varchar(1) DEFAULT NULL COMMENT '1:充电桩故障 2:充电桩离线 3:闪烁告警',\n" +
                "  `fault_state` varchar(1) DEFAULT NULL COMMENT '0:已恢复 1:故障中',\n" +
                "  `fault_start_time` datetime DEFAULT NULL COMMENT '故障开始时间',\n" +
                "  `fault_end_time` datetime DEFAULT NULL COMMENT '故障结束时间',\n" +
                "  `fault_receive_time` datetime DEFAULT NULL COMMENT '故障接受时间',\n" +
                "  `fault_receive_clear_time` datetime DEFAULT NULL COMMENT '故障清除时间',\n" +
                "  `fault_clear_mode` varchar(16) DEFAULT NULL COMMENT '故障清除方式',\n" +
                "  `fault_marking` varchar(4) DEFAULT NULL COMMENT '故障码',\n" +
                "  `fault_detail` varchar(255) DEFAULT NULL COMMENT '故障描述',\n" +
                "  `work_order_id` varchar(32) DEFAULT NULL COMMENT '工单ID',\n" +
                "  `work_order_state` varchar(1) DEFAULT NULL COMMENT '1:未派单 2:未接单 3:处理中4:办结',\n" +
                "  `work_order_start_time` datetime DEFAULT NULL COMMENT '工单派发时间',\n" +
                "  `work_order_end_time` datetime DEFAULT NULL COMMENT '工单办结时间',\n" +
                "  `work_order_receive_time` datetime DEFAULT NULL COMMENT '工单系统接收时间，处理多线程问题',\n" +
                "  `work_order_produce_time` datetime DEFAULT NULL COMMENT '工单接单时间',\n" +
                "  `work_order_user_id` varchar(32) DEFAULT NULL COMMENT '工单处理人',\n" +
                "  `sr_tag` varchar(2) DEFAULT NULL COMMENT '停复运标识',\n" +
                "  `opera_time` datetime DEFAULT NULL COMMENT '操作时间',\n" +
                "  `ac_tag` varchar(1) DEFAULT NULL COMMENT '通信状态标识 0 离线 1 在线',\n" +
                "  `ac_status` varchar(1) DEFAULT NULL COMMENT '故障产生时通信状态 0 离线 1 在线',\n" +
                "  `acc_duration` bigint(19) DEFAULT NULL COMMENT '故障累计时长',\n" +
                "  `is_delete` varchar(1) DEFAULT NULL COMMENT '删除标识 0 正常 1 标识',\n" +
                "  `fault_start_manage_status` varchar(2) DEFAULT NULL COMMENT '故障开始时管理状态',\n" +
                "  `fault_end_manage_status` varchar(2) DEFAULT NULL COMMENT '故障结束时管理状态',\n" +
                "  `is_span_day` varchar(1) DEFAULT NULL COMMENT '是否跨天 0 非跨天， 1跨天',\n" +
                "  `work_status` varchar(4) DEFAULT NULL COMMENT '工作状态',\n" +
                "  `fault_start_mq_detail` varchar(255) DEFAULT NULL COMMENT '故障产生时mq',\n" +
                "  `fault_end_mq_detail` varchar(255) DEFAULT NULL COMMENT '故障恢复时mq',\n" +
                "  `alarm_rule_detail` text COMMENT '告警规则',\n" +
                "  `alarm_rule_result` text COMMENT '告警规则匹配结果',\n" +
                "  `alarm_level` varchar(4) DEFAULT NULL COMMENT '告警等级 1 严重 2 一般 3 提示',\n" +
                "  `tcu_version` varchar(4) DEFAULT NULL COMMENT '充电桩tcu版本',\n" +
                "  `alarm_from` varchar(4) DEFAULT NULL COMMENT '故障来源',\n" +
                "  `charger_no` varchar(32) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`uuid`) USING BTREE\n" +
                ")";

        List<DDLEntry> list = ddlParser("oms_monitor","rtm_alarm","create",sql);
    }
}

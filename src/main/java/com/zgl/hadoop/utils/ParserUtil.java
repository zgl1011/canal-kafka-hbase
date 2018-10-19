package com.zgl.hadoop.utils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
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
            if (sqlStatement instanceof MySqlAlterTableStatement) {
                MySqlAlterTableStatement stmt = (MySqlAlterTableStatement) sqlStatement;
                List<SQLAlterTableItem> items = stmt.getItems();
                for (SQLAlterTableItem item : items) {
                    DDLColumn ddlColumn = new DDLColumn();

                    if (item instanceof MySqlAlterTableAddColumn) {
                        MySqlAlterTableAddColumn stmtOfAddCol = (MySqlAlterTableAddColumn) item;
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

                    if (element instanceof MySqlSQLColumnDefinition) {
                        String colName = ((MySqlSQLColumnDefinition) element).getName().getSimpleName();
                        ddlColumn.setKeyWord(KeyWordConstant.CREATE);
                        ddlColumn.setColumnName(replaceAll(colName));

                        if (tableElementSize == 1) {
                            ddlColumn.setKey(true);
                        }
                    }

                    if (element instanceof MySqlPrimaryKey) {
                        List<SQLExpr> exprList = ((MySqlPrimaryKey) element).getColumns();
                        for (SQLExpr expr : exprList) {
                            if (expr instanceof SQLIdentifierExpr) {
                                String name = ((SQLIdentifierExpr) expr).getName();
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
}

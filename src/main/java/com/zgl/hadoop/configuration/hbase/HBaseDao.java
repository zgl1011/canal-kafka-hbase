package com.zgl.hadoop.configuration.hbase;


import com.zgl.hadoop.utils.RandCodeEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import org.springframework.stereotype.Component;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zgl
 * \* Date: 2018-10-11
 * \* Time: 16:31
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
@Component
public class HBaseDao {
    private static final Logger logger = LoggerFactory.getLogger(HBaseDao.class);



    private Connection conn;

    /**
     * 获得链接
     * @return
     */
    public synchronized Connection getConnection() {
        if(conn == null || conn.isClosed()){
            conn = HBaseConnectionFactory.connection;
        }
        return conn;

    }

    /**
     * 关闭连接
     * @throws IOException
     */
    public static void closeConnect(Connection conn){
        if(null != conn){
            try {
                conn.close();
            } catch (Exception e) {
                logger.error("closeConnect failure !", e);
            }
        }
    }

    /**
     * 创建表空间
     * @param name
     * @throws IOException
     */
    public void createNamespace(String name) throws IOException {
        logger.info("createNamespace namespace:{}", name);
        HBaseAdmin admin = (HBaseAdmin) HBaseConnectionFactory.connection.getAdmin();
        admin.createNamespace(NamespaceDescriptor.create(name).build());
        admin.close();
    }

    public boolean hasNamespace(String name) {
        String ns = null;

        try {
            HBaseAdmin admin = (HBaseAdmin) HBaseConnectionFactory.connection.getAdmin();
            ns = admin.getNamespaceDescriptor(name).getName();
        } catch (IOException e) {
            logger.warn("hbase namespace is not exist, name:{}.", name);
        }

        return StringUtils.equalsIgnoreCase(ns, name);
    }

    /**
     * 创建表
     * @param tableName
     * @throws Exception
     */
    public void createTable(String tableName, List<String> columnFamilies, boolean preBuildRegion) throws Exception {
        if(preBuildRegion){
//            String[] s = new String[] { "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F" };
            String[] s = HBaseConnectionFactory.splits;
//            int partition = 16;
            byte[][] splitKeys = new byte[s.length - 1][];
            for (int i = 1; i < s.length; i++) {
                splitKeys[i - 1] = Bytes.toBytes(s[i - 1]);
            }
            createTable(tableName, columnFamilies, splitKeys);
        } else {
            createTable(tableName, columnFamilies);
        }
    }

    private void createTable(String tableName, int pNum, boolean only) throws Exception {
        String[] s = RandCodeEnum.HBASE_CHAR.getHbaseKeys(pNum,2,only);
        byte[][] splitKeys = new byte[pNum][];
        for (int i = 1; i <= pNum; i++) {
            splitKeys[i - 1] = Bytes.toBytes(s[i - 1]);
        }
        List<String> cfs = new ArrayList<>();
        cfs.add("events");
        createTable(tableName,cfs, splitKeys);
    }

    /**
     * 建表(预分区处理)
     * @param tableName
     * @param cfs
     * @throws IOException
     */
    private void createTable(String tableName, List<String> cfs, byte[][] splitKeys) throws Exception {
        Connection conn = getConnection();
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        try {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                logger.warn("Table: {} is exists!", tableName);
                return;
            }
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf:cfs) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
//                hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                hColumnDescriptor.setMaxVersions(1);
                tableDesc.addFamily(hColumnDescriptor);
            }
            admin.createTable(tableDesc, splitKeys);
            logger.info("Table: {} create success!", tableName);
        } finally {
            admin.close();
          //  closeConnect(conn);
        }
    }

    /**
     * 建表
     * @param tableName
     * @param cfs
     * @throws IOException
     */
    private void createTable(String tableName, List<String> cfs) throws Exception {
        Connection conn = getConnection();
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        try {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                logger.warn("Table: {} is exists!", tableName);
                return;
            }
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf:cfs) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
 //               hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                hColumnDescriptor.setMaxVersions(1);
                tableDesc.addFamily(hColumnDescriptor);
            }
            admin.createTable(tableDesc);
            logger.info("Table: {} create success!", tableName);
        } finally {
            admin.close();
           // closeConnect(conn);
        }
    }

    /**
     * 异步往指定表添加数据（场景：每秒需要存储上千行数据到hbase）
     * @param tablename  	    表名
     * @param puts	 			需要添加的数据
     * @return long		    返回执行时间
     * @throws IOException
     */
    @Async("myExecutor")
    public Future<Long> put(String tablename, List<Put> puts) throws Exception {
        long currentTime = System.currentTimeMillis();
        Connection conn = getConnection();
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    System.out.println("Failed to sent put " + e.getRow(i) + ".");
                    logger.error("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename))
                .listener(listener);
        params.writeBufferSize(5 * 1024 * 1024);

        final BufferedMutator mutator = conn.getBufferedMutator(params);
        try {
            mutator.mutate(puts);
            mutator.flush();
        } finally {
            mutator.close();
         //   closeConnect(conn);
        }
        return new AsyncResult<>(System.currentTimeMillis() - currentTime);
    }

    /**
     * 异步往指定表添加数据
     * @param tablename  	    表名
     * @param put	 			需要添加的数据
     * @return long			返回执行时间
     * @throws IOException
     */
    @Async("myExecutor")
    public Future<Long> put(String tablename, Put put) throws Exception {
        return put(tablename, Arrays.asList(put));
    }

    /**
     *
     * 往指定表添加数据（场景：每秒需要存储上千行数据到hbase）
     * @param tablename  	    表名
     * @param puts	 			需要添加的数据
     * @return long		    返回执行时间
     * @throws IOException
     *
    缓冲区仅在两种情况下会刷新
    - 显示刷新
    - 用户调用flushCommit()方法，把数据发送到服务器做永久存储。
    - 隐式刷新
    - 隐式刷新会在用户调用put或setWriteBufferSize()方法时触发。这两个方法都会将目前占用的缓冲区大小与用户配置的大小做比较，如果超出限制则会调用flushCommits()方法。
     */
    @Async("myExecutor")
    public Future<Long> putByHTable(String tablename, List<?> puts) throws Exception {
        long currentTime = System.currentTimeMillis();
        Connection conn = getConnection();
        HTable htable = (HTable) conn.getTable(TableName.valueOf(tablename));
        //开启客户端的写缓冲区，缓冲区负责收集put操作，然后调用RPC(每次.put(put)为一次RPC)操作一次性将put送往服务器。
        //将数据自动提交功能关闭
        //htable.setAutoFlushTo(false);
        //Put实例保存在客户端进程中的内存
        //设置数据缓存区域
      //  htable.setWriteBufferSize(5 * 1024 * 1024);
        try {
            htable.put((List<Put>)puts);
            //刷新缓存区
      //      htable.flushCommits();
        } finally {
            htable.close();
          //  closeConnect(conn);
        }
        return new AsyncResult<>(System.currentTimeMillis() - currentTime);
    }

    /**
     * 获取单行数据
     * @param tablename
     * @param row
     * @return
     * @throws IOException
     */
    @Async("myExecutor")
    public Future<Result> getRow(String tablename, byte[] row) {
        Table table = getTable(tablename);
        Result rs = null;
        if(table!=null){
            try{
                Get g = new Get(row);
                rs = table.get(g);
            } catch (IOException e) {
                logger.error("getRow failure !", e);
            } finally{
                try {
                    table.close();
                } catch (IOException e) {
                    logger.error("getRow failure !", e);
                }
            }
        }
        return new AsyncResult<>(rs);
    }

    /**
     * 获取多行数据
     * @param tablename
     * @param rows
     * @return
     * @throws Exception
     */
    @Async("myExecutor")
    public <T> Result[] getRows(String tablename, List<T> rows) {
        Table table = getTable(tablename);
        List<Get> gets = null;
        Result[] results = null;
        try {
            if (table != null) {
                gets = new ArrayList<Get>();
                for (T row : rows) {
                    if(row!=null){
                        gets.add(new Get(Bytes.toBytes(String.valueOf(row))));
                    }else{
                        throw new RuntimeException("hbase have no data");
                    }
                }
            }
            if (gets.size() > 0) {
                results = table.get(gets);
            }
        } catch (IOException e) {
            logger.error("getRows failure !", e);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                logger.error("table.close() failure !", e);
            }
        }
        return results;
    }

    /**
     * 检索指定表的第一行记录。<br>
     * （如果在创建表时为此表指定了非默认的命名空间，则需拼写上命名空间名称，格式为【namespace:tablename】）。
     * @param tableName 表名称(*)。
     * @param filterList 过滤器集合，可以为null。
     * @return
     */
    @Async("myExecutor")
    public Result selectFirstResultRow(String tableName,FilterList filterList) {
        if(StringUtils.isBlank(tableName)) return null;
        Table table = null;
        try {
            table = getTable(tableName);
            Scan scan = new Scan();
            if(filterList != null) {
                scan.setFilter(filterList);
            }
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            int index = 0;
            while(iterator.hasNext()) {
                Result rs = iterator.next();
                if(index == 0) {
                    scanner.close();
                    return rs;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 扫描整张表，注意使用完要释放。
     * @param tablename
     * @return
     * @throws IOException
     */
    @Async("myExecutor")
    public ResultScanner get(String tablename) {
        Table table = getTable(tablename);
        ResultScanner results = null;
        if (table != null) {
            try {
                Scan scan = new Scan();
                //从服务器一次抓取1000条数据
                scan.setCaching(1000);
                results = table.getScanner(scan);
            } catch (IOException e) {
                logger.error("getResultScanner failure !", e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    logger.error("table.close() failure !", e);
                }
            }
        }
        return results;
    }

    /**
     * 删除单行数据
     * @param tablename
     * @param row
     * @throws IOException
     */
    public void delete(String tablename, String row) {
        Table table = getTable(tablename);
        if(table!=null){
            try {
                Delete d = new Delete(row.getBytes());
                table.delete(d);
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 删除多行数据
     * @param tablename
     * @param rows
     * @throws IOException
     */
    public void delete(String tablename, String[] rows) throws IOException {
        Table table = getTable(tablename);
        if (table != null) {
            try {
                List<Delete> list = new ArrayList<Delete>();
                for (String row : rows) {
                    Delete d = new Delete(row.getBytes());
                    list.add(d);
                }
                if (list.size() > 0) {
                    table.delete(list);
                }
            } finally {
                table.close();
            }
        }
    }

    /**
     * 删除表
     * @param tablename
     * @throws IOException
     */
    public void deleteTable(String tablename) throws IOException {
        Connection conn = getConnection();
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        try {
            if (!admin.tableExists(TableName.valueOf(tablename))) {
                logger.warn("Table: {} is not exists!", tablename);
                return;
            }
            admin.disableTable(TableName.valueOf(tablename));
            admin.deleteTable(TableName.valueOf(tablename));
            logger.info("Table: {} delete success!", tablename);
        } finally {
            admin.close();
         //   closeConnect(conn);
        }
    }



    /**
     * 获取Table
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public Table getTable(String tableName){
        try {
            return getConnection().getTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            logger.error("Obtain Table failure !", e);
        }
        return null;
    }

    /**
     * 给table创建snapshot
     * @param snapshotName 快照名称
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public void snapshot(String snapshotName, TableName tableName){
        try {
            Admin admin = getConnection().getAdmin();
            admin.snapshot(snapshotName, tableName);
        } catch (Exception e) {
            logger.error("Snapshot " + snapshotName + " create failed !", e);
        }
    }

    /**
     * 获得现已有的快照
     * @param snapshotNameRegex 正则过滤表达式
     * @return
     * @throws IOException
     */
    public List<SnapshotDescription> listSnapshots(String snapshotNameRegex){
        try {
            Admin admin = getConnection().getAdmin();
            if(StringUtils.isNotBlank(snapshotNameRegex))
                return admin.listSnapshots(snapshotNameRegex);
            else
                return admin.listSnapshots();
        } catch (Exception e) {
            logger.error("Snapshot " + snapshotNameRegex + " get failed !", e);
        }
        return null;
    }

    /**
     * 批量删除Snapshot
     * @param snapshotNameRegex 正则过滤表达式
     * @return
     * @throws IOException
     */
    public void deleteSnapshots(String snapshotNameRegex){
        try {
            Admin admin = getConnection().getAdmin();
            if(StringUtils.isNotBlank(snapshotNameRegex))
                admin.deleteSnapshots(snapshotNameRegex);
            else
                logger.error("SnapshotNameRegex can't be null !");
        } catch (Exception e) {
            logger.error("Snapshots " + snapshotNameRegex + " del failed !", e);
        }
    }

    /**
     * 单个删除Snapshot
     * @param snapshotName 正则过滤表达式
     * @return
     * @throws IOException
     */
    public void deleteSnapshot(String snapshotName){
        try {
            Admin admin = getConnection().getAdmin();
            if(StringUtils.isNotBlank(snapshotName))
                admin.deleteSnapshot(snapshotName);
            else
                logger.error("SnapshotName can't be null !");
        } catch (Exception e) {
            logger.error("Snapshot " + snapshotName + " del failed !", e);
        }
    }

    /**
     * 分页检索表数据。<br>
     * （如果在创建表时为此表指定了非默认的命名空间，则需拼写上命名空间名称，格式为【namespace:tablename】）。
     * @param tableName 表名称(*)。
     * @param startRowKey 起始行键(可以为空，如果为空，则从表中第一行开始检索)。
     * @param endRowKey 结束行键(可以为空)。
     * @param filterList 检索条件过滤器集合(不包含分页过滤器；可以为空)。
     * @param maxVersions 指定最大版本数【如果为最大整数值，则检索所有版本；如果为最小整数值，则检索最新版本；否则只检索指定的版本数】。
     * @param pageModel 分页模型(*)。
     * @return 返回HBasePageModel分页对象。
     */
    @Async("myExecutor")
    public HBasePageModel scanResultByPageFilter(String tableName, byte[] startRowKey, byte[] endRowKey, FilterList filterList, int maxVersions, HBasePageModel pageModel) {
        if(pageModel == null) {
            pageModel = new HBasePageModel(10);
        }
        if(maxVersions <= 0 ) {
            //默认只检索数据的最新版本
            maxVersions = Integer.MIN_VALUE;
        }
        pageModel.initStartTime();
        pageModel.initEndTime();
        if(StringUtils.isBlank(tableName)) {
            return pageModel;
        }
        Table table = null;

        try {
            table = getTable(tableName);
            int tempPageSize = pageModel.getPageSize();
            boolean isEmptyStartRowKey = false;
            if(startRowKey == null) {
                //则读取表的第一行记录
                Result firstResult = selectFirstResultRow(tableName, filterList);
                if(firstResult.isEmpty()) {
                    return pageModel;
                }
                startRowKey = firstResult.getRow();
            }
            if(pageModel.getPageStartRowKey() == null) {
                isEmptyStartRowKey = true;
                pageModel.setPageStartRowKey(startRowKey);
            } else {
                if(pageModel.getPageEndRowKey() != null) {
                    pageModel.setPageStartRowKey(pageModel.getPageEndRowKey());
                }
                //从第二页开始，每次都多取一条记录，因为第一条记录是要删除的。
                tempPageSize += 1;
            }

            Scan scan = new Scan();
            scan.setStartRow(pageModel.getPageStartRowKey());
            if(endRowKey != null) {
                scan.setStopRow(endRowKey);
            }
            PageFilter pageFilter = new PageFilter(pageModel.getPageSize() + 1);
            if(filterList != null) {
                filterList.addFilter(pageFilter);
                scan.setFilter(filterList);
            } else {
                scan.setFilter(pageFilter);
            }
            if(maxVersions == Integer.MAX_VALUE) {
                scan.setMaxVersions();
            } else if(maxVersions == Integer.MIN_VALUE) {

            } else {
                scan.setMaxVersions(maxVersions);
            }
            ResultScanner scanner = table.getScanner(scan);
            List<Result> resultList = new ArrayList<Result>();
            int index = 0;
            for(Result rs : scanner.next(tempPageSize)) {
                if(isEmptyStartRowKey == false && index == 0) {
                    index += 1;
                    continue;
                }
                if(!rs.isEmpty()) {
                    resultList.add(rs);
                }
                index += 1;
            }
            scanner.close();
            pageModel.setResultList(resultList);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int pageIndex = pageModel.getPageIndex() + 1;
        pageModel.setPageIndex(pageIndex);
        if(pageModel.getResultList().size() > 0) {
            //获取本次分页数据首行和末行的行键信息
            byte[] pageStartRowKey = pageModel.getResultList().get(0).getRow();
            byte[] pageEndRowKey = pageModel.getResultList().get(pageModel.getResultList().size() - 1).getRow();
            pageModel.setPageStartRowKey(pageStartRowKey);
            pageModel.setPageEndRowKey(pageEndRowKey);
        }
        int queryTotalCount = pageModel.getQueryTotalCount() + pageModel.getResultList().size();
        pageModel.setQueryTotalCount(queryTotalCount);
        pageModel.initEndTime();
        pageModel.printTimeInfo();
        return pageModel;
    }

    /**
     * 格式化输出结果
     */
    public void formatRow(KeyValue[] rs){
        for(KeyValue kv : rs){
            System.out.print(" column-family:" + Bytes.toString(kv.getFamilyArray()));
            System.out.print(" column:" + Bytes.toString(kv.getQualifierArray()));
            System.out.print(" value:" + Bytes.toString(kv.getValueArray()));
            System.out.print(" timestamp:" + String.valueOf(kv.getTimestamp()));
            System.out.println("--------------------");
        }
    }

    /**
     * byte[] 类型的长整形数字转换成 long 类型
     * @param byteNum
     * @return
     */
    public long bytes2Long(byte[] byteNum) {
        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }

    public void addColumn(String tableName,List<String> columns) throws IOException {
        logger.info("add columns, tableName:{}, cols:{}", tableName, columns);
        Connection conn = getConnection();
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        if (columns == null || columns.isEmpty()) {
            return;
        }

        TableName tableName1 = TableName.valueOf(tableName);

        try {
            admin.disableTable(tableName1);

            HTableDescriptor hTableDescriptor = admin.getTableDescriptor(tableName1);

            HColumnDescriptor[] hColumnDescriptors = hTableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnDescriptor : hColumnDescriptors) {
                columns.remove(columnDescriptor.getNameAsString());
            }

            if (!columns.isEmpty()) {
                for (String col : columns) {
                    hTableDescriptor.addFamily(new HColumnDescriptor(col));
                }
            }

            admin.modifyTable(tableName1, hTableDescriptor);
        } catch (Exception e) {
            logger.error("hbase addCols failed. tableName:{}, cols:{}", tableName1, columns, e);
            throw e;
        } finally {
            admin.enableTable(tableName1);
            admin.close();
           // closeConnect(conn);
        }
    }

    public void delColumn(String tableName, List<String> columns) throws IOException {
        logger.info("del columns, tableName:{}, cols:{}", tableName, columns);
        Connection conn = getConnection();
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        if (columns == null || columns.isEmpty()) {
            return;
        }

        TableName tableName1 = TableName.valueOf(tableName);

        try {
            admin.disableTable(tableName1);

            HTableDescriptor hTableDescriptor = admin.getTableDescriptor(tableName1);
            for (String col : columns) {
                hTableDescriptor.removeFamily(col.getBytes());
            }

            admin.modifyTable(tableName1, hTableDescriptor);
        } catch (Exception e) {
            logger.error("hbase deleteCols failed. tableName:{}, cols:{}", tableName1, columns, e);
            throw e;
        } finally {
            admin.enableTable(tableName1);
            admin.close();
           // closeConnect(conn);
        }
    }
}

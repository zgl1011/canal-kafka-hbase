# canal-kafka-hbase
基于canal.deployer-1.1.1-SNAPSHOT.tar，canal连接kafka，消费kafka数据入hbase
## 说明
* 监听mysql binlog，canal的配置直接看canal官方文档
* 消费kafka的数据写入hbase，rowKey用主键hashCode取模的形式，hbase建表进行预分区

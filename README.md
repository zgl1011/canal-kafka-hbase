# canal-kafka-hbase
基于canal.deployer-1.1.1-SNAPSHOT.tar，canal连接kafka，消费kafka数据入hbase和ElasticSearch
## 说明
* 监听mysql binlog，canal的配置直接看canal官方文档
* 消费kafka的数据写入hbase，rowKey用主键hashCode取模的形式，hbase建表进行预分区
* 入hbase的同时双写入ElasticSearch,实现二级索引
新增了增量数据和带有事物处理的增量（新增、修改、删除）的区分操作，纯粹的增量数据回写kafka走batch消费，ElasticSearch通过BULK写入和hbase通过批量，增强写入的效率

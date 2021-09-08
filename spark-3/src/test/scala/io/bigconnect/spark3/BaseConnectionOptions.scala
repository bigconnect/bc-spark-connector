package io.bigconnect.spark3

import io.bigconnect.spark.util.BcOptions

trait BaseConnectionOptions {
  def optionMap(): scala.collection.Map[String, String] = Map(
    BcOptions.GRAPH_SEARCH_LOCATIONS -> "localhost",
    BcOptions.GRAPH_SEARCH_CLUSTERNAME -> "bdl",
    BcOptions.GRAPH_SEARCH_PORT -> "9300",
    BcOptions.GRAPH_SEARCH_INDEXNAME -> ".ge",
    BcOptions.GRAPH_ZOOKEEPERS -> "localhost:2181",
    BcOptions.GRAPH_HDFS_ROOT_DIR -> "hdfs://localhost:9000",
    BcOptions.GRAPH_HDFS_DATA_DIR -> "/bigconnect/data",
    BcOptions.GRAPH_HDFS_USER -> "flavius",
    BcOptions.GRAPH_HDFS_CONF_DIR -> "/opt/bdl/etc/hadoop",
    BcOptions.GRAPH_ACCUMULO_INSTANCE -> "accumulo",
    BcOptions.GRAPH_ACCUMULO_PREFIX -> "bc",
    BcOptions.GRAPH_ACCUMULO_USER -> "root",
    BcOptions.GRAPH_ACCUMULO_PASSWORD -> "secret",
    BcOptions.PARTITIONING_STRATEGY -> BcOptions.PARTITIONING_STRATEGY_SHARD
  )
}

package io.bigconnect.spark

import io.bigconnect.spark.util.{BcMapping, BcOptions}
import org.junit.Assert.{assertEquals, fail}
import org.junit.Test

class DataSourceReaderTest extends SparkConnectorScalaBaseTest {
  @Test
  def testThrowsExceptionIfNoValidReadOptionIsSet(): Unit = {
    try {
      sparkSession().read
        .format(classOf[DataSource].getName)
        .load()
    } catch {
      case e: IllegalArgumentException =>
        assertEquals("Parameter 'graph.search.locations' is required", e.getMessage)
      case _: Throwable => fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}")
    }
  }

  @Test
  def testCountVertices(): Unit = {
    val df = sparkSession().read.format(classOf[DataSource].getName)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_VERTEX)
      .load()

    df.count()
  }

  @Test
  def testCountEdges(): Unit = {
    val df = sparkSession().read.format(classOf[DataSource].getName)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_EDGE)
      .load()

    df.count()
  }

  @Test
  def testFilterVertex(): Unit = {
    val df = sparkSession().read.format(classOf[DataSource].getName)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_VERTEX)
      .option(BcOptions.PARTITIONING_STRATEGY, BcOptions.PARTITIONING_STRATEGY_SPLIT)
      .load()
      .filter(s"${BcMapping.CONCEPT_TYPE_FIELD} = 'bankAccount' AND balance > 10")
    println(df.count())
  }

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
    BcOptions.GRAPH_ACCUMULO_PASSWORD -> "secret"
  )
}

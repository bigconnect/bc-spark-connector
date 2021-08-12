package io.bigconnect.spark3

import com.mware.core.model.schema.SchemaConstants
import io.bigconnect.spark.SparkConnectorScalaBaseTest
import io.bigconnect.spark.util.{BcMapping, BcOptions}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test

case class Person(name: String, surname: String, age: Int)

class DataSourceWriterTest extends SparkConnectorScalaBaseTest {
  private val spark: SparkSession = sparkSession()

  import spark.implicits._

  @Test
  def `should write nodes with string values`(): Unit = {
    val df = (1 to 10)
      .map(i => i.toString)
      .toDF("prop1")

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_VERTEX)
      .save()
  }

  @Test
  def `should write edges with string values`(): Unit = {
      val df = Seq(
        ("100000", "200000", SchemaConstants.EDGE_LABEL_HAS_ENTITY, "str1"),
        ("200000", "300000", SchemaConstants.EDGE_LABEL_HAS_ENTITY, "str2"),
      ).toDF(BcMapping.SOURCE_ID_FIELD, BcMapping.DEST_ID_FIELD, BcMapping.EDGE_LABEL_FIELD, "prop1")

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_EDGE)
      .options(optionMap())
      .save()
  }

  @Test
  def `should write within partitions`(): Unit = {
    val ds = (1 to 100).map(i => Person("Andrea " + i, "Santurbano " + i, 36))
      .toDS()
      .repartition(10)

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_VERTEX)
      .save()
  }

  @Test
  def `should write with schema`(): Unit = {
    val df = Seq(
      (12, "John Bonham", "Drums"),
      (19, "John Mayer", "Guitar"),
      (32, "John Scofield", "Guitar"),
      (15, "John Butler", "Guitar")
    ).toDF("experience", "title", "instrument")

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_VERTEX)
      .save()
  }

  def optionMap(): scala.collection.Map[String, String] = Map(
    BcOptions.GRAPH_SEARCH_LOCATIONS -> "localhost",
    BcOptions.GRAPH_SEARCH_CLUSTERNAME -> "bdl",
    BcOptions.GRAPH_SEARCH_PORT -> "9300",
    BcOptions.GRAPH_SEARCH_INDEXNAME -> "test_ge",
    BcOptions.GRAPH_ZOOKEEPERS -> "localhost:2181",
    BcOptions.GRAPH_HDFS_ROOT_DIR -> "hdfs://localhost:9000",
    BcOptions.GRAPH_HDFS_DATA_DIR -> "/bigconnect/data",
    BcOptions.GRAPH_HDFS_USER -> "flavius",
    BcOptions.GRAPH_HDFS_CONF_DIR -> "/opt/bdl/etc/hadoop",
    BcOptions.GRAPH_ACCUMULO_INSTANCE -> "accumulo",
    BcOptions.GRAPH_ACCUMULO_PREFIX -> "test",
    BcOptions.GRAPH_ACCUMULO_USER -> "root",
    BcOptions.GRAPH_ACCUMULO_PASSWORD -> "secret"
  )
}

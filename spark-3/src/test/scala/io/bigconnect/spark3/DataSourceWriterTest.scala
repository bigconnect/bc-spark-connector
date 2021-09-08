package io.bigconnect.spark3

import com.mware.core.model.schema.SchemaConstants
import io.bigconnect.spark.SparkConnectorScalaBaseTest
import io.bigconnect.spark.util.{BcMapping, BcOptions}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.Assert.assertEquals
import org.junit.Test

import java.time.ZoneOffset

case class Person(name: String, surname: String, age: Int)

class DataSourceWriterTest extends SparkConnectorScalaBaseTest with BaseConnectionOptions {
  import spark.implicits._

  private def testType[T](ds: DataFrame, neo4jType: Type): Unit = {
    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_VERTEX)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.foo AS foo
        |""".stripMargin).list().asScala
      .filter(r => r.get("foo").hasType(neo4jType))
      .map(r => r.asMap().asScala)
      .toSet
    val expected = ds.collect()
      .map(row => Map("foo" -> {
        val foo = row.getAs[T]("foo")
        foo match {
          case sqlDate: java.sql.Date => sqlDate
            .toLocalDate
          case sqlTimestamp: java.sql.Timestamp => sqlTimestamp.toInstant
            .atZone(ZoneOffset.UTC)
          case _ => foo
        }
      }))
      .toSet
    assertEquals(expected, records)
  }

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
}

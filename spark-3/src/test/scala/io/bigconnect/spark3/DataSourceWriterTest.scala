package io.bigconnect.spark3

import com.mware.bigconnect.driver.internal.types.InternalTypeSystem
import com.mware.bigconnect.driver.types.{IsoDuration, Type}
import com.mware.core.model.schema.SchemaConstants
import io.bigconnect.spark.SparkConnectorScalaBaseTest
import io.bigconnect.spark.util.{BcMapping, BcOptions}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import java.time.ZoneOffset
import scala.collection.JavaConverters._

case class Person(name: String, surname: String, age: Int)
case class SimplePerson(name: String, surname: String)
case class EmptyRow[T](data: T)
abstract class BcType(`type`: String)
case class Duration(`type`: String = "duration",
                    months: Long,
                    days: Long,
                    seconds: Long,
                    nanoseconds: Long) extends BcType(`type`)

class DataSourceWriterTest extends SparkConnectorScalaBaseTest with BaseConnectionOptions {

  import spark.implicits._

  private def testType[T](ds: DataFrame, cypherType: Type): Unit = {
    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_VERTEX)
      .option(BcOptions.CONCEPT_TYPE, SchemaConstants.CONCEPT_TYPE_THING)
      .save()

    val records = SparkConnectorScalaBaseTest.bcSession().run(
      """MATCH (p:thing)
        |RETURN p.foo AS foo
        |""".stripMargin).list().asScala
      .filter(r => r.get("foo").hasType(cypherType))
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

  private def testArray[T](ds: DataFrame): Unit = {
    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_VERTEX)
      .option(BcOptions.CONCEPT_TYPE, SchemaConstants.CONCEPT_TYPE_THING)
      .save()

    val records = SparkConnectorScalaBaseTest.bcSession().run(
      """MATCH (p:thing)
        |RETURN p.foo AS foo
        |""".stripMargin).list().asScala
      .filter(r => r.get("foo").hasType(InternalTypeSystem.TYPE_SYSTEM.LIST()))
      .map(r => r.get("foo").asList())
      .toSet
    val expected = ds.collect()
      .map(row => row.getList[T](0))
      .toSet
    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with string array values`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => i.toString)
      .map(i => Array(i, i))
      .toDF("foo")

    testArray[String](ds)
  }

  @Test
  def `should write nodes with string values`(): Unit = {
    val ds = (1 to 10)
      .map(i => i.toString)
      .toDF("foo")

    testType[String](ds, InternalTypeSystem.TYPE_SYSTEM.STRING())
  }

  @Test
  def `should write nodes with int values`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => i)
      .toDF("foo")

    testType[Int](ds, InternalTypeSystem.TYPE_SYSTEM.INTEGER())
  }

  @Test
  def `should write nodes with date values`(): Unit = {
    val total = 5
    val ds = (1 to total)
      .map(i => java.sql.Date.valueOf("2020-01-0" + i))
      .toDF("foo")

    testType[java.sql.Date](ds, InternalTypeSystem.TYPE_SYSTEM.DATE())
  }

  @Test
  def `should write nodes with timestamp values`(): Unit = {
    val total = 5
    val ds = (1 to total)
      .map(i => java.sql.Timestamp.valueOf(s"2020-01-0$i 11:11:11.11"))
      .toDF("foo")

    testType[java.sql.Timestamp](ds, InternalTypeSystem.TYPE_SYSTEM.DATE_TIME())
  }

  @Test
  def `should write nodes with int array values`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => i.toLong)
      .map(i => Array(i, i))
      .toDF("foo")

    testArray[Long](ds)
  }

  @Test
  def `should write nodes with duration values`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => i.toLong)
      .map(i => EmptyRow(Duration(months = i, days = i, seconds = i, nanoseconds = i)))
      .toDS()

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_VERTEX)
      .option(BcOptions.CONCEPT_TYPE, "BeanWithDuration")
      .save()

    val records = SparkConnectorScalaBaseTest.bcSession().run(
      """MATCH (p:BeanWithDuration)
        |RETURN p.data AS data
        |""".stripMargin).list().asScala
      .map(r => r.get("data").asIsoDuration())
      .map(data => (data.months, data.days, data.seconds, data.nanoseconds))
      .toSet

    val expected = ds.collect()
      .map(row => (row.data.months, row.data.days, row.data.seconds, row.data.nanoseconds))
      .toSet

    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with duration array values`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => i.toLong)
      .map(i => EmptyRow(Seq(Duration(months = i, days = i, seconds = i, nanoseconds = i),
        Duration(months = i, days = i, seconds = i, nanoseconds = i))))
      .toDS()

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_VERTEX)
      .option(BcOptions.CONCEPT_TYPE, "BeanWithDuration")
      .save()

    val records = SparkConnectorScalaBaseTest.bcSession().run(
      """MATCH (p:BeanWithDuration)
        |RETURN p.data AS data
        |""".stripMargin).list().asScala
      .map(r => r.get("data")
        .asList.asScala
        .map(_.asInstanceOf[IsoDuration])
        .map(data => (data.months, data.days, data.seconds, data.nanoseconds)))
      .toSet

    val expected = ds.collect()
      .map(row => row.data.map(data => (data.months, data.days, data.seconds, data.nanoseconds)))
      .toSet

    assertEquals(expected, records)
  }

  @Test
  def `handle null properties`(): Unit = {
    val ds = Seq(SimplePerson("Andrea", null)).toDS()

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, BcMapping.TYPE_VERTEX)
      .option(BcOptions.CONCEPT_TYPE, "Person")
      .save()

    val nodeList = SparkConnectorScalaBaseTest.bcSession()
      .run(
        """MATCH (n:Person{name: 'Andrea'})
          |RETURN n
          |""".stripMargin)
      .list()
      .asScala
    assertEquals(1, nodeList.size)
    val node = nodeList.head.get("n").asNode()
    assertTrue("surname should not exist", node.asMap().get("surname") == null)
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

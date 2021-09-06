package io.bigconnect.spark3

import io.bigconnect.spark.SparkConnectorScalaBaseTest
import io.bigconnect.spark.util.{BcMapping, BcOptions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.junit.Assert.{assertEquals, assertNull, assertTrue, fail}
import org.junit.Test

import java.sql.Timestamp
import java.time.{LocalDateTime, OffsetDateTime, ZoneOffset}
import scala.collection.mutable

class DataSourceReaderTest extends SparkConnectorScalaBaseTest {
  @Test
  def testThrowsExceptionIfNoValidReadOptionIsSet(): Unit = {
    try {
      spark.read
        .format(classOf[DataSource].getName)
        .load()
        .show()
    } catch {
      case e: IllegalArgumentException =>
        assertTrue(e.getMessage.matches("Parameter '.*' is required"))
      case _: Throwable => fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}")
    }
  }

  @Test
  def testReadNodeHasIdField(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {name: 'John'})")
    val _id = df.select(BcMapping.ID_FIELD).collectAsList().get(0).getString(0)
    assertTrue(_id.toLong > -1)
  }

  @Test
  def testReadNodeHasLabelsField(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {name: 'John'}) CREATE (c:Customer { name: 'Jake'})")

    val result = df.select(BcMapping.CONCEPT_TYPE_FIELD)
      .collect()
      .map(r => r.getString(0))

    assertTrue(result.contains("Person"))
    assertTrue(result.contains("Customer"))
  }

  @Test
  def testReadNodeWithFieldWithDifferentTypes(): Unit = {
    val df: DataFrame = initTest("CREATE (p1:Person {__id: 1, field1: [12,34]}), (p2:Person {__id: 2, field2: 123})")

    val res = df.orderBy(BcMapping.ID_FIELD).collectAsList()

    val arr = mutable.WrappedArray.newBuilder.+=(12, 34)
    assertEquals(arr, res.get(0).getAs("field1"))
    assertEquals(123L, res.get(1).getAs("field2"))
  }

  @Test
  def testReadNodeWithString(): Unit = {
    val name: String = "John"
    val df: DataFrame = initTest(s"CREATE (p:Person {name: '$name'})")

    assertEquals(name, df.select("name").collectAsList().get(0).get(0))
  }

  @Test
  def testReadNodeWithDouble(): Unit = {
    val score: Double = 3.14
    val df: DataFrame = initTest(s"CREATE (p:Person {score: $score})")

    assertEquals(score, df.select("score").collectAsList().get(0).getDouble(0), 0)
  }

  @Test
  def testReadNodeWithLocalDateTime(): Unit = {
    val localDateTime = "2007-12-03T10:15:30"
    val df: DataFrame = initTest(s"CREATE (p:Person {ldtTime: localdatetime('$localDateTime')})")

    val result = df.select("ldtTime").collectAsList().get(0).getTimestamp(0)

    assertEquals(Timestamp.from(LocalDateTime.parse(localDateTime).toInstant(ZoneOffset.UTC)), result)
  }

  @Test
  def testReadNodeWithZonedDateTime(): Unit = {
    val datetime = "2015-06-24T12:50:35.556+01:00"
    val df: DataFrame = initTest(s"CREATE (p:Person {zdtTime: datetime('$datetime')})")

    val result = df.select("zdtTime").collectAsList().get(0).getTimestamp(0)

    assertEquals(Timestamp.from(OffsetDateTime.parse(datetime).toInstant), result)
  }

  @Test
  def testReadNodeWithDate(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {born: date('2009-10-10')})")

    val list = df.select("born").collectAsList()
    val res = list.get(0).getDate(0)

    assertEquals(java.sql.Date.valueOf("2009-10-10"), res)
  }

  @Test
  def testReadNodeWithDuration(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {range: duration({days: 14, hours:16, minutes: 12})})")

    val list = df.select("range").collectAsList()
    val res = list.get(0).getAs[GenericRowWithSchema](0)

    assertEquals("duration", res(0))
    assertEquals(0L, res(1))
    assertEquals(14L, res(2))
    assertEquals(58320L, res(3))
    assertEquals(0, res(4))
    assertEquals("P14DT16H12M", res(5))
  }

  @Test
  def testReadNodeWithStringArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {names: ['John', 'Doe']})")

    val res = df.select("names").collectAsList().get(0).getAs[Seq[String]](0)

    assertEquals("John", res.head)
    assertEquals("Doe", res(1))
  }

  @Test
  def testReadNodeWithLongArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {ages: [22, 23]})")

    val res = df.select("ages").collectAsList().get(0).getAs[Seq[Long]](0)

    assertEquals(22, res.head)
    assertEquals(23, res(1))
  }

  @Test
  def testReadNodeWithDoubleArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {scores: [22.33, 44.55]})")

    val res = df.select("scores").collectAsList().get(0).getAs[Seq[Double]](0)

    assertEquals(22.33, res.head, 0)
    assertEquals(44.55, res(1), 0)
  }

  @Test
  def testReadNodeWithBooleanArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {bools: [true, false]})")

    val res = df.select("bools").collectAsList().get(0).getAs[Seq[Boolean]](0)

    assertEquals(true, res.head)
    assertEquals(false, res(1))
  }

  @Test
  def testReadNodeWithArrayDate(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {dates: [date('2009-10-10'), date('2009-10-11')]})")

    val res = df.select("dates").collectAsList().get(0).getAs[Seq[java.sql.Date]](0)

    assertEquals(java.sql.Date.valueOf("2009-10-10"), res.head)
    assertEquals(java.sql.Date.valueOf("2009-10-11"), res(1))
  }

  @Test
  def testReadNodeWithArrayZonedDateTime(): Unit = {
    val datetime1 = "2015-06-24T12:50:35.556+01:00"
    val datetime2 = "2015-06-23T12:50:35.556+01:00"
    val df: DataFrame = initTest(
      s"""
     CREATE (p:Person {aTime: [
      datetime('$datetime1'),
      datetime('$datetime2')
     ]})
     """)

    val result = df.select("aTime").collectAsList().get(0).getAs[Seq[Timestamp]](0)

    assertEquals(Timestamp.from(OffsetDateTime.parse(datetime1).toInstant), result.head)
    assertEquals(Timestamp.from(OffsetDateTime.parse(datetime2).toInstant), result(1))
  }

  @Test
  def testReadNodeWithArrayDurations(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {durations: [duration({months: 0.75}), duration({weeks: 2.5})]})")

    val res = df.select("durations").collectAsList().get(0).getAs[Seq[GenericRowWithSchema]](0)

    assertEquals("duration", res.head.get(0))
    assertEquals(0L, res.head.get(1))
    assertEquals(22L, res.head.get(2))
    assertEquals(71509L, res.head.get(3))
    assertEquals(500000000, res.head.get(4))
    assertEquals("P22DT19H51M49.5S", res.head.get(5))

    assertEquals("duration", res(1).get(0))
    assertEquals(0L, res(1).get(1))
    assertEquals(17L, res(1).get(2))
    assertEquals(43200L, res(1).get(3))
    assertEquals(0, res(1).get(4))
    assertEquals("P17DT12H", res(1).get(5))
  }

  @Test
  def testReadNodeWithEqualToFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {name: 'John Doe'}),
      (p2:Person {name: 'Jane Doe'})
     """)

    val result = df.select("name").where("name = 'John Doe'").collectAsList()

    assertEquals(1, result.size())
    assertEquals("John Doe", result.get(0).getString(0))
  }

  @Test
  def testReadNodeWithEqualToDateFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {birth: date('1998-02-04')}),
      (p2:Person {birth: date('1988-01-05')})
     """)

    val result = df.select("birth").where("birth = '1988-01-05'").collectAsList()

    assertEquals(1, result.size())
    assertEquals(java.sql.Date.valueOf("1988-01-05"), result.get(0).getDate(0))
  }

  @Test
  def testReadNodeWithEqualToTimestampFilter(): Unit = {
    val localDateTime = "2007-12-03T10:15:30"
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {birth: localdatetime('$localDateTime')}),
      (p2:Person {birth: localdatetime('$localDateTime')})
     """)

    df.printSchema()
    df.show()

    val result = df.select("birth").where(s"birth >= '$localDateTime'").collectAsList()
    assertEquals(2, result.size())
  }

  @Test
  def testReadNodeWithNotEqualToFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {name: 'John Doe'}),
      (p2:Person {name: 'Jane Doe'})
     """)

    val result = df.select("name").where("NOT name = 'John Doe'").collectAsList()

    assertEquals(1, result.size())
    assertEquals("Jane Doe", result.get(0).getString(0))
  }

  @Test
  def testReadNodeWithNotEqualToDateFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {birth: date('1998-02-04')}),
      (p2:Person {birth: date('1988-01-05')})
     """)

    val result = df.select("birth").where("NOT birth = '1988-01-05'").collectAsList()

    assertEquals(1, result.size())
    assertEquals(java.sql.Date.valueOf("1998-02-04"), result.get(0).getDate(0))
  }

  @Test
  def testReadNodeWithDifferentOperatorFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {name: 'John Doe'}),
      (p2:Person {name: 'Jane Doe'})
     """)

    val result = df.select("name").where("name != 'John Doe'").collectAsList()

    assertEquals(1, result.size())
    assertEquals("Jane Doe", result.get(0).getString(0))
  }

  @Test
  def testReadNodeWithGtFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {age: 19}),
      (p2:Person {age: 20}),
      (p3:Person {age: 21})
     """)

    val result = df.select("age").where("age > 20").collectAsList()

    assertEquals(1, result.size())
    assertEquals(21, result.get(0).getLong(0))
  }

  @Test
  def testReadNodeWithGtDateFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {birth: date('1998-02-04')}),
      (p2:Person {birth: date('1988-01-05')}),
      (p3:Person {birth: date('1994-10-16')})
     """)

    val result = df.select("birth").orderBy("birth").where("birth > '1990-01-01'").collectAsList()

    assertEquals(2, result.size())
    assertEquals(java.sql.Date.valueOf("1994-10-16"), result.get(0).getDate(0))
    assertEquals(java.sql.Date.valueOf("1998-02-04"), result.get(1).getDate(0))
  }

  @Test
  def testReadNodeWithGteFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {age: 19}),
      (p2:Person {age: 20}),
      (p3:Person {age: 21})
     """)

    val result = df.select("age").orderBy("age").where("age >= 20").collectAsList()

    assertEquals(2, result.size())
    assertEquals(20, result.get(0).getLong(0))
    assertEquals(21, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithGteFilterWithProp(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {score: 19, limit: 20}),
      (p2:Person {score: 20,  limit: 18}),
      (p3:Person {score: 21,  limit: 12})
     """)

    val result = df.select("score").orderBy("score").where("score >= limit").collectAsList()

    assertEquals(2, result.size())
    assertEquals(20, result.get(0).getLong(0))
    assertEquals(21, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithLtFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: 41}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").orderBy("age").where("age < 40").collectAsList()

    assertEquals(1, result.size())
    assertEquals(39, result.get(0).getLong(0))
  }

  @Test
  def testReadNodeWithLteFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: 41}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").orderBy("age").where("age <= 41").collectAsList()

    assertEquals(2, result.size())
    assertEquals(39, result.get(0).getLong(0))
    assertEquals(41, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithInFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: 41}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").orderBy("age").where("age IN(41,43)").collectAsList()

    assertEquals(2, result.size())
    assertEquals(41, result.get(0).getLong(0))
    assertEquals(43, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithIsNullFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: null}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").where("age IS NULL").collectAsList()

    assertEquals(1, result.size())
    assertNull(result.get(0).get(0))
  }

  @Test
  def testReadNodeWithIsNotNullFilter(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: null}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").orderBy("age").where("age IS NOT NULL").collectAsList()

    assertEquals(2, result.size())
    assertEquals(39, result.get(0).getLong(0))
    assertEquals(43, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithOrCondition(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: null}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").orderBy("age").where("age = 43 OR age = 39 OR age = 32").collectAsList()

    assertEquals(2, result.size())
    assertEquals(39, result.get(0).getLong(0))
    assertEquals(43, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithAndCondition(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: null}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").orderBy("age").where("age >= 39 AND age <= 43").collectAsList()

    assertEquals(2, result.size())
    assertEquals(39, result.get(0).getLong(0))
    assertEquals(43, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithStartsWith(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {name: 'John Mayer'}),
      (p2:Person {name: 'John Scofield'}),
      (p3:Person {name: 'John Butler'})
     """)

    val result = df.select("name").orderBy("name").where("name LIKE 'John%'").collectAsList()

    assertEquals(3, result.size())
    assertEquals("John Butler", result.get(0).getString(0))
    assertEquals("John Mayer", result.get(1).getString(0))
    assertEquals("John Scofield", result.get(2).getString(0))
  }

  @Test
  def testReadNodeWithEndsWith(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {name: 'John Mayer'}),
      (p2:Person {name: 'John Scofield'}),
      (p3:Person {name: 'John Butler'})
     """)

    val result = df.select("name").where("name LIKE '%Scofield'").collectAsList()

    assertEquals(1, result.size())
    assertEquals("John Scofield", result.get(0).getString(0))
  }

  @Test
  def testReadNodeWithContains(): Unit = {
    val df: DataFrame = initTest(
      s"""
     CREATE (p1:Person {name: 'John Mayer'}),
      (p2:Person {name: 'John Scofield'}),
      (p3:Person {name: 'John Butler'})
     """)

    val result = df.select("name").where("name LIKE '%ay%'").collectAsList()

    assertEquals(1, result.size())
    assertEquals("John Mayer", result.get(0).getString(0))
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
    BcOptions.GRAPH_ACCUMULO_PASSWORD -> "secret",
    BcOptions.PARTITIONING_STRATEGY -> BcOptions.PARTITIONING_STRATEGY_SHARD
  )

  private def initTest(query: String, elementType: String = BcMapping.TYPE_VERTEX): DataFrame = {
    SparkConnectorScalaBaseTest.bcSession()
      .run(query).consume()

    spark.read.format(classOf[DataSource].getName)
      .options(optionMap())
      .option(BcOptions.ELEMENT_TYPE, elementType)
      .load()
  }
}

package io.bigconnect.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.rules.TestName
import org.junit.{AfterClass, BeforeClass, Rule}

object SparkConnectorScalaBaseTest {
  var conf: SparkConf = _
  var ss: SparkSession = _

  @BeforeClass
  def setUpSpark(): Unit = {
    conf = new SparkConf()
      .setAppName("bcTest")
      .setMaster("local[*]")
      .set("spark.driver.host", "127.0.0.1")

    ss = SparkSession.builder.config(conf).getOrCreate()
  }

  @AfterClass
  def tearDownSpark() = {
    TestUtil.closeSafety(ss)
  }
}

class SparkConnectorScalaBaseTest {
  val _testName: TestName = new TestName

  @Rule
  def testName = _testName

  def sparkSession(): SparkSession = SparkConnectorScalaBaseTest.ss
}

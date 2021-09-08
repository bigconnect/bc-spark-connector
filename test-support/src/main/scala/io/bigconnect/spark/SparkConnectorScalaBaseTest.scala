package io.bigconnect.spark

import com.mware.bigconnect.driver.Logging.slf4j
import com.mware.bigconnect.driver._
import com.mware.core.model.schema.SchemaConstants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.rules.TestName
import org.junit.{AfterClass, Before, BeforeClass, Rule}

import java.net.URI

object SparkConnectorScalaBaseTest {
  var conf: SparkConf = _
  var ss: SparkSession = _
  var driver: Driver = _
  var session: Session = _

  @BeforeClass
  def setUpSpark(): Unit = {
    conf = new SparkConf()
      .setAppName("bcTest")
      .setMaster("local[*]")
      .set("spark.driver.host", "127.0.0.1")

    ss = SparkSession.builder.config(conf).getOrCreate()
    driver = BigConnect.driver(
      URI.create("bolt://localhost:10242"),
      AuthTokens.basic("admin", "admin"),
      Config.builder()
        .withoutEncryption()
        .withTrustStrategy(Config.TrustStrategy.trustAllCertificates())
        .withLogging(slf4j())
        .build()
    )
  }

  def bcSession(): Session = {
    if (session == null || !session.isOpen) {
      session = driver.session
    }
    session
  }

  @AfterClass
  def tearDownSpark() = {
    TestUtil.closeSafety(bcSession())
    TestUtil.closeSafety(driver)
    TestUtil.closeSafety(ss)
  }
}

class SparkConnectorScalaBaseTest {
  val _testName: TestName = new TestName
  val spark = SparkConnectorScalaBaseTest.ss

  @Rule
  def testName = _testName

  @Before
  def before(): Unit = {
    val bcSession = SparkConnectorScalaBaseTest.bcSession()

    bcSession.run("MATCH (n) DELETE n")
      .consume()

    bcSession.run(
      "CALL db.propertyKeys() YIELD propertyKey " +
        "CALL schema.deleteProperty('public-ontology', propertyKey) " +
        "RETURN propertyKey"
    ).consume()

    bcSession.run(
      "CALL db.schemaRelationships() YIELD name " +
        "CALL schema.deleteRelationship('public-ontology', name) " +
        "RETURN name"
    ).consume()

    bcSession.run(
      s"CALL db.schemaConcepts() YIELD name WITH name WHERE name <> '${SchemaConstants.CONCEPT_TYPE_THING}' " +
        "CALL schema.deleteConcept('public-ontology', name) " +
        "RETURN name"
    ).consume()
  }
}

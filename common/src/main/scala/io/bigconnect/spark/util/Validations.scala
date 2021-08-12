package io.bigconnect.spark.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object Validations extends Logging {
  def version(supportedVersions: String*): Unit = {
    val sparkVersion = SparkSession.getActiveSession
      .map { _.version }
      .getOrElse("UNKNOWN")

    ValidationUtil.isTrue(
      sparkVersion == "UNKNOWN" || supportedVersions.exists(sparkVersion.matches),
      s"""Your currentSpark version ${sparkVersion} is not supported by the current connector.
         |Please visit https://docs.bigconnect.io/biggraph/spark to know which connector version you need.
         |""".stripMargin
    )
  }
}

package io.bigconnect.spark3

import io.bigconnect.spark.util.{BcOptions, Validations}
import io.bigconnect.spark3.reader.BcScanBuilder
import io.bigconnect.spark3.writer.BcWriterBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class BcTable(schema: StructType, options: java.util.Map[String, String], jobId: String) extends Table
  with SupportsRead
  with SupportsWrite
  with Logging {

  private val bcOptions = new BcOptions(options)

  override def name(): String = bcOptions.elementType

  override def schema(): StructType = schema

  override def capabilities(): java.util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.OVERWRITE_DYNAMIC,
    TableCapability.OVERWRITE_BY_FILTER,
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new BcScanBuilder(bcOptions, jobId, schema())

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new BcWriterBuilder(bcOptions, jobId, schema(), SaveMode.Append)
}

package io.bigconnect.spark

import io.bigconnect.spark.reader.BcDataSourceReader
import io.bigconnect.spark.util.{BcOptions, Validations}
import io.bigconnect.spark.writer.BcDataSourceWriter
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

import java.util.{Optional, UUID}

class DataSource extends DataSourceV2
  with ReadSupport
  with DataSourceRegister
  with WriteSupport {

  Validations.version("2.4.*")

  private val jobId: String = UUID.randomUUID().toString

  override def shortName(): String = "bigconnect"

  override def createReader(options: DataSourceOptions): DataSourceReader = new BcDataSourceReader(new BcOptions(options.asMap()), jobId)
  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = new BcDataSourceReader(new BcOptions(options.asMap()), jobId, schema)

  override def createWriter(jobId: String, schema: StructType, saveMode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new BcDataSourceWriter(jobId, schema, saveMode, new BcOptions(options.asMap())))
  }
}

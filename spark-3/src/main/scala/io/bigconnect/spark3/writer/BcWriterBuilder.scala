package io.bigconnect.spark3.writer

import io.bigconnect.spark.util.BcOptions
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsOverwrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class BcWriterBuilder(bcOptions: BcOptions, jobId: String, schema: StructType, saveMode: SaveMode)
  extends WriteBuilder with SupportsOverwrite with SupportsTruncate {
  override def buildForBatch(): BatchWrite = new BcBatchWriter(bcOptions, jobId, schema, saveMode)

  override def overwrite(filters: Array[Filter]): WriteBuilder = new BcWriterBuilder(bcOptions, jobId, schema, SaveMode.Overwrite)
}

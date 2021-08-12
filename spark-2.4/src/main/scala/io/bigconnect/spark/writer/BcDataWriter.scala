package io.bigconnect.spark.writer

import io.bigconnect.spark.util.BcOptions
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.DataWriter
import org.apache.spark.sql.types.StructType

class BcDataWriter(partitionId: Int, jobId: String, schema: StructType, saveMode: SaveMode, options: BcOptions)
  extends BaseDataWriter(partitionId, jobId, schema, saveMode, options)
    with DataWriter[InternalRow]

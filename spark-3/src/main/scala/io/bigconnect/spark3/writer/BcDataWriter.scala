package io.bigconnect.spark3.writer

import io.bigconnect.spark.util.BcOptions
import io.bigconnect.spark.writer.BaseDataWriter
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.types.StructType

class BcDataWriter(jobId: String,
                   partitionId: Int,
                   schema: StructType,
                   saveMode: SaveMode,
                   options: BcOptions)
  extends BaseDataWriter(partitionId, jobId, schema, saveMode, options)
    with DataWriter[InternalRow]

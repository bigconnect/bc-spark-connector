package io.bigconnect.spark3.writer

import io.bigconnect.spark.util.BcOptions
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

class BcDataWriterFactory(jobId: String,
                          structType: StructType,
                          saveMode: SaveMode,
                          options: BcOptions) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = new BcDataWriter(
    jobId,
    partitionId,
    structType,
    saveMode,
    options,
  )
}

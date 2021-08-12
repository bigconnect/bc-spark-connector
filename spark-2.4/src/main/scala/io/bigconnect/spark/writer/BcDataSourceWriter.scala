package io.bigconnect.spark.writer

import io.bigconnect.spark.util.BcOptions
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class BcDataSourceWriter(jobId: String, schema: StructType, saveMode: SaveMode, options: BcOptions) extends DataSourceWriter with Serializable {
  override def createWriterFactory(): DataWriterFactory[InternalRow] =
    new BcWriterFactory(schema, saveMode, options)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // nothing to do
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // nothing to do
  }

  class BcWriterFactory(schema: StructType, saveMode: SaveMode, options: BcOptions) extends DataWriterFactory[InternalRow] {
    override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
      new BcDataWriter(partitionId, jobId, schema, saveMode, options)
    }
  }
}

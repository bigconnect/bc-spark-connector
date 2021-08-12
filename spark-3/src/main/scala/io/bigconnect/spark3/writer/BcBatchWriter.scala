package io.bigconnect.spark3.writer

import io.bigconnect.spark.util.BcOptions
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class BcBatchWriter(bcOptions: BcOptions, jobId: String, structType: StructType, saveMode: SaveMode) extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new BcDataWriterFactory(jobId, structType, saveMode, bcOptions)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // do nothing
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // do nothing
  }
}

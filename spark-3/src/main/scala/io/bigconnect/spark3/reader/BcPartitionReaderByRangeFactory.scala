package io.bigconnect.spark3.reader

import io.bigconnect.spark.util.BcOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class BcPartitionReaderByRangeFactory(private val bcOptions: BcOptions,
                                      private val filters: Array[Filter],
                                      private val schema: StructType,
                                      private val jobId: String,
                                      private val requiredColumns: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new BcPartitionReaderByRange(
      bcOptions,
      filters,
      schema,
      jobId,
      partition.asInstanceOf[BcPartition].ranges.get,
      requiredColumns
    )
  }
}

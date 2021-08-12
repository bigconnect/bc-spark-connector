package io.bigconnect.spark3.reader

import io.bigconnect.spark.reader.BasePartitionReaderByShard
import io.bigconnect.spark.util.BcOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class BcPartitionReaderByShard(private val options: BcOptions,
                                private val filters: Array[Filter],
                                private val schema: StructType,
                                private val jobId: String,
                                private val shard: String,
                                private val requiredColumns: StructType)
  extends BasePartitionReaderByShard(options, shard, filters, jobId, schema, requiredColumns)
    with PartitionReader[InternalRow]

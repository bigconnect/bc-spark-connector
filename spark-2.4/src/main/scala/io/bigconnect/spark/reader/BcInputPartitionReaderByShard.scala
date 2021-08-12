package io.bigconnect.spark.reader

import io.bigconnect.spark.util.BcOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType

@SerialVersionUID(1L)
class BcInputPartitionReaderByShard(private val options: BcOptions,
                                    private val shard: String,
                                    private val filters: Array[Filter],
                                    private val schema: StructType,
                                    private val jobId: String,
                                    private val requiredColumns: StructType
                                   )
  extends BasePartitionReaderByShard(options, shard, filters, jobId, schema, requiredColumns)
    with InputPartitionReader[InternalRow]
    with Serializable

package io.bigconnect.spark.reader

import io.bigconnect.spark.util.BcOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType

@SerialVersionUID(1L)
class BcInputPartitionReaderByRange(private val options: BcOptions,
                                    private val ranges: Seq[Seq[Array[Byte]]],
                                    private val filters: Array[Filter],
                                    private val schema: StructType,
                                    private val jobId: String,
                                    private val requiredColumns: StructType
                            )
  extends BasePartitionReaderByRange(options, ranges, filters, jobId, schema, requiredColumns)
    with InputPartitionReader[InternalRow]
    with Serializable

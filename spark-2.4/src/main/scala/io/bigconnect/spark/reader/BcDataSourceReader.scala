package io.bigconnect.spark.reader

import com.mware.ge.ElementType
import com.mware.ge.elasticsearch5.{Elasticsearch5SearchIndex, ElasticsearchSearchIndexConfiguration}
import com.mware.ge.store.AbstractStorableGraph
import io.bigconnect.spark.util.{BcMapping, BcOptions, BcUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{InputPartition, _}
import org.apache.spark.sql.types.StructType
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(1L)
class BcDataSourceReader(private val bcOptions: BcOptions, private val jobId: String, private val userDefinedSchema: StructType = null)
  extends DataSourceReader
    with Serializable
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private var filters: Array[Filter] = Array[Filter]()
  private var requiredColumns: StructType = new StructType()

  private val structType = if (userDefinedSchema != null) {
    userDefinedSchema
  } else {
    BcMapping.structFromSchema(bcOptions.configuration.toBcConfig, ElementType.parse(bcOptions.elementType))
  }

  override def readSchema(): StructType = structType

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    if (bcOptions.partitioningStrategy == BcOptions.PARTITIONING_STRATEGY_SHARD)
      planInputPartitionsByShards()
    else
      planInputPartitionsBySplits()
  }

  def planInputPartitionsByShards(): util.List[InputPartition[InternalRow]] = {
    // plan by elastic shards
    val graphConfig = BcUtil.createGraphConfiguration(bcOptions.configuration.toBcConfig);
    val transportClient = Elasticsearch5SearchIndex.createTransportClient(new ElasticsearchSearchIndexConfiguration(null, graphConfig))
    val healthResponse = transportClient.admin().cluster().health(new ClusterHealthRequest()).actionGet()
    val shards = healthResponse.getActivePrimaryShards
    val foo = (0 to shards)
      .map(shard => new PartitionReaderByShardFactory(shard.toString))
      .asJava

    new java.util.ArrayList[InputPartition[InternalRow]](foo)
  }

  def planInputPartitionsBySplits(): util.List[InputPartition[InternalRow]] = {
    val splits = ArrayBuffer(Array.empty[Byte], Array.empty[Byte])

    val graphConfig = BcUtil.createGraphConfiguration(bcOptions.configuration.toBcConfig);
    val client = BcUtil.createGraphConfiguration(bcOptions.configuration.toBcConfig).createConnector();

    val tableName = bcOptions.elementType match {
      case BcMapping.TYPE_VERTEX => AbstractStorableGraph.getVerticesTableName(graphConfig.getTableNamePrefix)
      case BcMapping.TYPE_EDGE => AbstractStorableGraph.getEdgesTableName(graphConfig.getTableNamePrefix)
    }

    val tableSplits = client.tableOperations().listSplits(tableName)
    // on deployed clusters a table with no split will return a single empty Text instance
    val containsSingleEmptySplit =
      tableSplits.size == 1 &&
        tableSplits.iterator.next.getLength == 0

    if (tableSplits.size > 1 || !containsSingleEmptySplit)
      splits.insertAll(1, tableSplits.asScala.map(_.getBytes))

    // convert splits to ranges
    var ranges = splits.sliding(2).toSeq

    // optionally shuffle
    ranges = scala.util.Random.shuffle(ranges)

    // create groups of ranges
    val numReaders = scala.math.min(ranges.length, bcOptions.maxPartitions)
    val batchSize = ranges.length / numReaders
    val batchRanges = ranges.sliding(batchSize, batchSize)

    val logger = Logger.getLogger(classOf[BcDataSourceReader])
    logger.info(s"Splits '$batchRanges' creating $numReaders readers")

    val foo = batchRanges.map(r => new PartitionReaderByRangeFactory(r))
      .toSeq.asJava

    new java.util.ArrayList[InputPartition[InternalRow]](foo)
  }

  override def pushFilters(filtersArray: Array[Filter]): Array[Filter] = {
    filters = filtersArray
    filtersArray
  }

  override def pushedFilters(): Array[Filter] = filters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    requiredColumns = if (requiredSchema == userDefinedSchema) {
      new StructType()
    } else {
      requiredSchema
    }
  }

  class PartitionReaderByRangeFactory(ranges: Seq[Seq[Array[Byte]]])
    extends InputPartition[InternalRow] {

    override def createPartitionReader(): InputPartitionReader[InternalRow] = {
      new BcInputPartitionReaderByRange(bcOptions, ranges, filters, structType, jobId, requiredColumns)
    }
  }

  class PartitionReaderByShardFactory(shard: String)
    extends InputPartition[InternalRow] {

    override def createPartitionReader(): InputPartitionReader[InternalRow] = {
      new BcInputPartitionReaderByShard(bcOptions, shard, filters, structType, jobId, requiredColumns)
    }
  }
}

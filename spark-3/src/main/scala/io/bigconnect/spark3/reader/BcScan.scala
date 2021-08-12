package io.bigconnect.spark3.reader

import com.mware.ge.elasticsearch5.{Elasticsearch5SearchIndex, ElasticsearchSearchIndexConfiguration}
import com.mware.ge.store.AbstractStorableGraph
import io.bigconnect.spark.util.{BcMapping, BcOptions, BcUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class BcPartition(ranges: Option[Seq[Seq[Array[Byte]]]], shard: Option[String]) extends InputPartition

class BcScan(bcOptions: BcOptions,
             jobId: String,
             schema: StructType,
             filters: Array[Filter],
             requiredColumns: StructType
                ) extends Scan with Batch {


  override def toBatch: Batch = this

  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    if (bcOptions.partitioningStrategy == BcOptions.PARTITIONING_STRATEGY_SHARD)
      planInputPartitionsByShards()
    else
      planInputPartitionsBySplits()
  }

  def planInputPartitionsByShards(): Array[InputPartition] = {
    // plan by elastic shards
    val graphConfig = BcUtil.createGraphConfiguration(bcOptions.configuration.toBcConfig);
    val transportClient = Elasticsearch5SearchIndex.createTransportClient(new ElasticsearchSearchIndexConfiguration(null, graphConfig))
    val healthResponse = transportClient.admin().cluster().health(new ClusterHealthRequest()).actionGet()
    val shards = healthResponse.getActivePrimaryShards
    (0 to shards)
      .map(shard => BcPartition(Option.empty, Option.apply(shard.toString)))
      .toArray
  }

  def planInputPartitionsBySplits(): Array[InputPartition] = {
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

    val logger = Logger.getLogger(classOf[BcScan])
    logger.info(s"Splits '$batchRanges' creating $numReaders readers")

    batchRanges.map(r => BcPartition(Option.apply(r), Option.empty))
      .toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    if (bcOptions.partitioningStrategy == BcOptions.PARTITIONING_STRATEGY_SHARD)
      new BcPartitionReaderByShardFactory(bcOptions, filters, schema, jobId, requiredColumns)
    else
      new BcPartitionReaderByRangeFactory(bcOptions, filters, schema, jobId, requiredColumns)
  }
}

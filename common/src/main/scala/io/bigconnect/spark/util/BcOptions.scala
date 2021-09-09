  package io.bigconnect.spark.util

import com.mware.core.config.{Configuration, HashMapConfigurationLoader}
import com.mware.core.model.schema.SchemaConstants
import com.mware.ge.accumulo.AccumuloGraphConfiguration
import com.mware.ge.{Authorizations, FetchHints}
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.JavaConverters._

class BcOptions(private val options: java.util.Map[String, String]) extends Serializable {
  import BcOptions._

  val maxPartitions = 200
  val elementType: String = getRequiredParameter(ELEMENT_TYPE)
  val authorizations: Authorizations = if (getParameter(AUTHORIZATIONS, DEFAULT_EMPTY).isEmpty) {
    new Authorizations
  } else {
    new Authorizations(getParameter(AUTHORIZATIONS, DEFAULT_EMPTY))
  }
  val conceptType:String = getParameter(CONCEPT_TYPE, SchemaConstants.CONCEPT_TYPE_THING)
  val labelType:String = getParameter(LABEL_TYPE, SchemaConstants.EDGE_LABEL_HAS_ENTITY)
  val fetchHints: FetchHints = getParameter(FETCH_HINTS, DEFAULT_FETCH_HINTS) match {
    case "ALL" => FetchHints.ALL
    case "PROPERTIES" => FetchHints.PROPERTIES
    case "PROPERTIES_AND_METADATA" => FetchHints.PROPERTIES_AND_METADATA
    case "PROPERTIES_AND_EDGE_REFS" => FetchHints.PROPERTIES_AND_EDGE_REFS
    case "ALL_INCLUDING_HIDDEN" => FetchHints.ALL_INCLUDING_HIDDEN
    case "EDGE_LABELS" => FetchHints.EDGE_LABELS
    case "EDGE_REFS" => FetchHints.EDGE_REFS
    case _ => FetchHints.NONE
  }
  val batchSize: Int = getParameter(BATCH_SIZE, DEFAULT_BATCH_SIZE.toString).toInt
  val partitioningStrategy: String = getParameter(PARTITIONING_STRATEGY, PARTITIONING_STRATEGY_SHARD)

  val configuration: BcGraphOptions = BcGraphOptions(
    getRequiredParameter(GRAPH_SEARCH_LOCATIONS),
    getRequiredParameter(GRAPH_SEARCH_CLUSTERNAME),
    getParameter(GRAPH_SEARCH_PORT, DEFAULT_SEARCH_PORT),
    getRequiredParameter(GRAPH_SEARCH_INDEXNAME),
    getRequiredParameter(GRAPH_ZOOKEEPERS),
    getRequiredParameter(GRAPH_HDFS_ROOT_DIR),
    getParameter(GRAPH_HDFS_DATA_DIR, DEFAULT_HDFS_DATADIR),
    getParameter(GRAPH_HDFS_USER, DEFAULT_HDFS_USER),
    getParameter(GRAPH_HDFS_CONF_DIR, DEFAULT_HDFS_CONFDIR),
    getParameter(GRAPH_ACCUMULO_INSTANCE, DEFAULT_ACCUMULO_INSTANCE),
    getParameter(GRAPH_ACCUMULO_PREFIX, DEFAULT_ACCUMULO_PREFIX),
    getParameter(GRAPH_ACCUMULO_USER, DEFAULT_ACCUMULO_USER),
    getParameter(GRAPH_ACCUMULO_PASSWORD, DEFAULT_ACCUMULO_PASSWORD),
    getParameter(GRAPH_BATCHWRITER_THREADS, DEFAULT_BATCHWRITER_MAX_THREADS),
    getParameter(GRAPH_BATCHWRITER_MAX_MEMORY, DEFAULT_BATCHWRITER_MAX_MEMORY)
  )

  private def parameters: util.Map[String, String] = {
    val sparkOptions = SparkSession.getActiveSession
      .map {
        _.conf
          .getAll
          .filterKeys(k => k.startsWith("bc."))
          .map { elem => (elem._1.substring("bc.".length + 1), elem._2) }
          .toMap
      }
      .getOrElse(Map.empty)


    (sparkOptions ++ options.asScala).asJava
  }

  private def getRequiredParameter(parameter: String): String = {
    if (!parameters.containsKey(parameter) || parameters.get(parameter).isEmpty) {
      throw new IllegalArgumentException(s"Parameter '$parameter' is required")
    }

    parameters.get(parameter)
  }

  private def getParameter(parameter: String, defaultValue: String = ""): String = {
    if (!parameters.containsKey(parameter) || parameters.get(parameter).isEmpty) {
      return defaultValue
    }

    parameters.get(parameter).trim()
  }

  def validate(validationFunction: BcOptions => Unit): BcOptions = {
    validationFunction(this)
    this
  }
}

object BcOptions {
  // element type
  val ELEMENT_TYPE = "element.type"
  val AUTHORIZATIONS = "authorizations"
  val FETCH_HINTS = "reader.fetchHints"
  val BATCH_SIZE = "writer.batchSize"
  val CONCEPT_TYPE = "conceptType"
  val LABEL_TYPE = "labelType"

  // connection options
  val GRAPH_SEARCH = "graph.search"
  val GRAPH_SEARCH_LOCATIONS = "graph.search.locations"
  val GRAPH_SEARCH_CLUSTERNAME = "graph.search.clustername"
  val GRAPH_SEARCH_PORT = "graph.search.port"
  val GRAPH_SEARCH_INDEXNAME = "graph.search.indexname"

  val GRAPH_ZOOKEEPERS = "graph.zookeeperservers"
  val GRAPH_HDFS_ROOT_DIR = "graph.hdfs.rootdir"
  val GRAPH_HDFS_DATA_DIR = "graph.hdfs.datadir"
  val GRAPH_HDFS_USER = "graph.hdfs.user"
  val GRAPH_HDFS_CONF_DIR = "graph.hdfs.confdir"
  val GRAPH_ACCUMULO_INSTANCE = "graph.accumuloinstancename"
  val GRAPH_ACCUMULO_PREFIX = "graph.tablenameprefix"
  val GRAPH_ACCUMULO_USER = "graph.username"
  val GRAPH_ACCUMULO_PASSWORD = "graph.password"
  val GRAPH_BATCHWRITER_THREADS = "batchwriter.maxWriteThreads"
  val GRAPH_BATCHWRITER_MAX_MEMORY = "batchwriter.maxMemory"

  // partitioning strategy
  val PARTITIONING_STRATEGY = "partitioning.strategy"

  // defaults
  val DEFAULT_EMPTY = ""
  val DEFAULT_FETCH_HINTS: String = "ALL"
  val DEFAULT_PARTITIONS = 1
  val DEFAULT_BATCH_SIZE = 5000
  val DEFAULT_BATCHWRITER_MAX_THREADS = s"${AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_MAX_WRITE_THREADS}"
  val DEFAULT_BATCHWRITER_MAX_MEMORY = s"${AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_MAX_MEMORY}"

  val DEFAULT_SEARCH_PORT = "9300"
  val DEFAULT_PUSHDOWN_FILTERS_ENABLED = true
  val DEFAULT_PUSHDOWN_COLUMNS_ENABLED = true
  val DEFAULT_HDFS_DATADIR = "/bigconnect/data"
  val DEFAULT_HDFS_CONFDIR = "/opt/bdl/etc/hadoop"
  val DEFAULT_HDFS_USER = "hdfs"
  val DEFAULT_ACCUMULO_INSTANCE = "accumulo"
  val DEFAULT_ACCUMULO_PREFIX = "bc"
  val DEFAULT_ACCUMULO_USER = "root"
  val DEFAULT_ACCUMULO_PASSWORD = "secret"
  val PARTITIONING_STRATEGY_SHARD = "shard"
  val PARTITIONING_STRATEGY_SPLIT = "range"
}

case class BcGraphOptions(searchLocations: String,
                          searchClusterName: String,
                          searchPort: String,
                          searchIndex: String,
                          zookeeperServers: String,
                          hdfsRootDir: String,
                          hdfsDataDir: String,
                          hdfsUser: String,
                          hdfsConfDir: String,
                          accumuloInstanceName: String,
                          tableNamePrefix: String,
                          username: String,
                          password: String,
                          batchWriterThreads: String,
                          batchWriterMaxMemory: String
                         ) extends Serializable {
  def toBcConfig: Configuration = {
    val config = new util.HashMap[String, Object]()
    config.put("repository.workQueue", "com.mware.core.model.workQueue.InMemoryWorkQueueRepository")
    config.put("repository.webQueue", "com.mware.core.model.workQueue.InMemoryWebQueueRepository")
    config.put("repository.graphAuthorization", "com.mware.core.model.graph.AccumuloGraphAuthorizationRepository")

    config.put("graph", "com.mware.ge.accumulo.AccumuloGraph")
    config.put("simpleOrmSession", "com.mware.core.orm.accumulo.AccumuloSimpleOrmSession")

    config.put("graph.search", "com.mware.ge.elasticsearch5.Elasticsearch5SearchIndex")
    config.put("graph.search.sidecar", "false")
    config.put("graph.search.shards", "8")
    config.put("graph.search.replicas", "0")
    config.put("graph.search.splitEdgesAndVertices", "false")
    config.put("graph.search.queryPagingLimit", "50000")
    config.put("graph.search.queryPageSize", "50000")

    config.put("graph.zookeeperServers", zookeeperServers)
    config.put("graph.hdfs.rootDir", hdfsRootDir)
    config.put("graph.hdfs.dataDir", hdfsDataDir)
    config.put("graph.hdfs.user", hdfsUser)
    config.put("graph.accumuloInstanceName", accumuloInstanceName)
    config.put("graph.tableNamePrefix", tableNamePrefix)
    config.put("graph.username", username)
    config.put("graph.password", password)
    config.put("graph.batchwriter.maxWriteThreads", batchWriterThreads)
    config.put("graph.batchwriter.maxMemory", batchWriterMaxMemory)

    config.put("simpleOrm.accumulo.instanceName", accumuloInstanceName)
    config.put("simpleOrm.accumulo.username", username)
    config.put("simpleOrm.accumulo.password", password)
    config.put("simpleOrm.accumulo.zookeeperServerNames", zookeeperServers)
    config.put("simpleOrm.accumulo.tablePrefix", s"${tableNamePrefix}_")

    config.put("graph.search.locations", searchLocations)
    config.put("graph.search.clusterName", searchClusterName)
    config.put("graph.search.port", searchPort)
    config.put("graph.search.indexName", searchIndex)
    new HashMapConfigurationLoader(config).createConfiguration()
  }
}


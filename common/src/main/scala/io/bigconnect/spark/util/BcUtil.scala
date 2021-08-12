package io.bigconnect.spark.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.mware.core.cache.InMemoryCacheService
import com.mware.core.config.Configuration
import com.mware.core.lifecycle.LifeSupportService
import com.mware.core.model.graph.{AccumuloGraphAuthorizationRepository, GraphRepository}
import com.mware.core.model.lock.SingleJvmLockRepository
import com.mware.core.model.schema.{GeSchemaRepository, SchemaRepository}
import com.mware.core.model.termMention.TermMentionRepository
import com.mware.core.model.workQueue._
import com.mware.core.security.DirectVisibilityTranslator
import com.mware.ge.accumulo.{AccumuloGraph, AccumuloGraphConfiguration}

object BcUtil {
  var schemaRepository: SchemaRepository = _;

  def createGraphConfiguration(config: Configuration): AccumuloGraphConfiguration = {
    new AccumuloGraphConfiguration(config.getSubset("graph").asInstanceOf[java.util.Map[String, Object]])
  }

  def createGraph(config: Configuration): AccumuloGraph = {
    AccumuloGraph.create(createGraphConfiguration(config))
  }

  def createSchemaRepository(graph: AccumuloGraph, config: Configuration): SchemaRepository = {
    val visibilityTranslator = new DirectVisibilityTranslator
    val wkqr = createWorkQueueRepository(graph, config)
    val wbqr = createWebQueueRepository(graph, config)
    val authorizationRepository = createGraphAuthorizationRepository(graph)
    val tmr = new TermMentionRepository(graph, authorizationRepository)
    val graphRepository = new GraphRepository(graph, visibilityTranslator, tmr, wkqr, wbqr, config);
    new GeSchemaRepository(
      graph, graphRepository, visibilityTranslator, config, authorizationRepository, new InMemoryCacheService()
    )
  }

  def createGraphAuthorizationRepository(graph: AccumuloGraph): AccumuloGraphAuthorizationRepository = {
    val authorizationRepository = new AccumuloGraphAuthorizationRepository
    authorizationRepository.setGraph(graph)
    authorizationRepository.setLockRepository(new SingleJvmLockRepository)
    authorizationRepository
  }

  private def createWorkQueueRepository(graph: AccumuloGraph, config: Configuration): WorkQueueRepository = {
    val clazz = config.getClass(Configuration.WORK_QUEUE_REPOSITORY, classOf[InMemoryWorkQueueRepository])

    if (clazz.isAssignableFrom(classOf[InMemoryWorkQueueRepository]))
      new InMemoryWorkQueueRepository(graph, config)
    else if (clazz.isAssignableFrom(classOf[RabbitMQWorkQueueRepository]))
      new RabbitMQWorkQueueRepository(graph, config, new LifeSupportService)
    else
      throw new IllegalArgumentException("Could not create a WorkQueueRepository of type: " + clazz.getName)
  }

  private def createWebQueueRepository(graph: AccumuloGraph, config: Configuration): WebQueueRepository = {
    val clazz = config.getClass(Configuration.WEB_QUEUE_REPOSITORY, classOf[InMemoryWebQueueRepository])

    if (clazz.isAssignableFrom(classOf[InMemoryWebQueueRepository]))
      new InMemoryWebQueueRepository
    else if (clazz.isAssignableFrom(classOf[RabbitMQWorkQueueRepository]))
      new RabbitMQWebQueueRepository(config, new LifeSupportService)
    else
      throw new IllegalArgumentException("Could not create a WorkQueueRepository of type: " + clazz.getName)
  }
}

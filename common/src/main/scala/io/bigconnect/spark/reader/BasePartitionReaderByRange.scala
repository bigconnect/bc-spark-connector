package io.bigconnect.spark.reader

import com.mware.ge.accumulo.AccumuloGraph
import com.mware.ge.values.storable.{Value, Values}
import com.mware.ge.{Direction, Edge, Element, Vertex}
import io.bigconnect.spark.util.BcMapping._
import io.bigconnect.spark.util.{BcMapping, BcOptions, BcUtil}
import org.apache.accumulo.core.data.{Key, Range}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable

class BasePartitionReaderByRange(private val options: BcOptions,
                                 private val ranges: Seq[Seq[Array[Byte]]],
                                 private val filters: Array[Filter],
                                 private val jobId: String,
                                 private val schema: StructType,
                                 private val requiredColumns: StructType) extends Logging {

  private val graph = BcUtil.createGraph(options.configuration.toBcConfig)

  private val scanner = options.elementType match {
    case BcMapping.TYPE_VERTEX => graph.createVertexScanner(options.fetchHints, 1, null, null, null, options.authorizations)
    case BcMapping.TYPE_EDGE => graph.createEdgeScanner(options.fetchHints, 1, null, null, null, options.authorizations)
  }

  private val scannerIterator = scanner.iterator()

  def next(): Boolean = scannerIterator.hasNext

  def get(): InternalRow = {
    val next = scannerIterator.next();
    val element: Element = options.elementType match {
      case BcMapping.TYPE_VERTEX => AccumuloGraph.createVertexFromIteratorValue(graph, next.getKey, next.getValue, options.fetchHints, options.authorizations)
      case BcMapping.TYPE_EDGE => AccumuloGraph.createEdgeFromEdgeIteratorValue(graph, next.getKey, next.getValue, options.fetchHints, options.authorizations)
    }
    val propNames = element.getProperties.asScala
      .map(p => p.getName)

    val valueMap: mutable.Map[String, Value] = new java.util.HashMap[String, Value].asScala
    valueMap.put(ID_FIELD, Values.stringValue(element.getId))

    options.elementType match {
      case BcMapping.TYPE_VERTEX => {
        valueMap.put(CONCEPT_TYPE_FIELD, Values.stringValue(element.asInstanceOf[Vertex].getConceptType))
      }
      case BcMapping.TYPE_EDGE => {
        valueMap.put(EDGE_LABEL_FIELD, Values.stringValue(element.asInstanceOf[Edge].getLabel))
        valueMap.put(SOURCE_ID_FIELD, Values.stringValue(element.asInstanceOf[Edge].getVertexId(Direction.OUT)))
        valueMap.put(DEST_ID_FIELD, Values.stringValue(element.asInstanceOf[Edge].getVertexId(Direction.IN)))
      }
    }

    propNames.foreach(p => valueMap.put(p, element.getPropertyValue(p)))
    convertToInternalRow(valueMap, schema)
  }

  def close(): Unit = {
    if (scanner != null)
      scanner.close()

    graph.shutdown()
  }
}

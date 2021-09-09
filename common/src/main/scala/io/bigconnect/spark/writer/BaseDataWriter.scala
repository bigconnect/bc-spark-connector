package io.bigconnect.spark.writer

import com.mware.ge.mutation.ElementMutation
import com.mware.ge.values.storable.TextValue
import com.mware.ge.{Element, Visibility}
import io.bigconnect.spark.util.BcMapping._
import io.bigconnect.spark.util.{BcMapping, BcOptions, BcUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import java.util

class BaseDataWriter(partitionId: Int, jobId: String, schema: StructType, saveMode: SaveMode, options: BcOptions) extends Logging {
  private val configuration = options.configuration.toBcConfig
  private val batch: util.List[ElementMutation[_ <: Element]] = new util.ArrayList[ElementMutation[_ <: Element]]()

  private val graph = BcUtil.createGraph(configuration)
  graph.getSearchIndex.enableBulkIngest(true)

  BcMapping.ensureSchemaCreated(options.configuration.toBcConfig, schema)
  BcMapping.ensureElementTypeExists(options)

  private def writeBatch(): Unit = {
    graph.saveElementMutations(batch, options.authorizations)
    batch.clear()
  }

  def write(row: InternalRow): Unit = {
    var m: ElementMutation[_ <: Element] = null

    if (options.elementType == BcMapping.TYPE_EDGE) {
      m = prepareEdge(row)
    } else {
      m = prepareVertex(row)
    }

    batch.add(BcMapping.convertFromInternalRow(row, schema, m))
    if (batch.size() == options.batchSize) {
      writeBatch()
    }
  }

  def prepareEdge(row: InternalRow): ElementMutation[_ <: Element] = {
    val sid = BcMapping.getColValue(row, schema, SOURCE_ID_FIELD)
    if (sid.isPresent)
      throw new IllegalArgumentException(s"Schema does not contain ${BcMapping.SOURCE_ID_FIELD} column for edge");
    val sourceId: String = sid.get()
      .asInstanceOf[TextValue]
      .stringValue()
    val did = BcMapping.getColValue(row, schema, DEST_ID_FIELD)
    if (did.isPresent)
      throw new IllegalArgumentException(s"Schema does not contain ${BcMapping.DEST_ID_FIELD} column for edge")
    val destId: String = did
      .asInstanceOf[TextValue]
      .stringValue()

    if (schema.fieldNames.contains(ID_FIELD)) {
      val idf =  BcMapping.getColValue(row, schema, ID_FIELD)
      if (idf.isPresent)
        throw new IllegalArgumentException(s"No ${ID_FIELD} field found")
      val id: String = idf
        .asInstanceOf[TextValue]
        .stringValue()
      graph.prepareEdge(id, sourceId, destId, options.labelType, Visibility.EMPTY)
    } else {
       graph.prepareEdge(sourceId, destId, options.labelType, Visibility.EMPTY)
    }
  }

  def prepareVertex(row: InternalRow): ElementMutation[_ <: Element] = {
    if (schema.fieldNames.contains(ID_FIELD)) {
      // this is an update
      val idf =  BcMapping.getColValue(row, schema, ID_FIELD)
      if (idf.isPresent)
        throw new IllegalArgumentException(s"No ${ID_FIELD} field found")
      val id: String = idf
        .asInstanceOf[TextValue]
        .stringValue()

      graph.prepareVertex(id, Visibility.EMPTY, options.conceptType)
    } else {
      // new vertex
      graph.prepareVertex(Visibility.EMPTY, options.conceptType)
    }
  }

  def commit(): Null = {
    writeBatch()
    close()
    null
  }

  def abort(): Unit = {
    close()
  }

  protected def close(): Unit = {
    if (graph != null && graph.getSearchIndex != null) {
      graph.getSearchIndex.enableBulkIngest(false)
      graph.flush()
      graph.shutdown()
    }
  }
}

package io.bigconnect.spark.reader

import com.mware.ge.{Direction, Edge, Element, ElementType, Vertex}
import com.mware.ge.query.{Compare, Contains, Query, QueryResultsIterable, TextPredicate}
import com.mware.ge.query.builder.GeQueryBuilders.{boolQuery, exists, hasConceptType, hasEdgeLabel, hasFilter, hasIds, searchAll}
import com.mware.ge.query.builder.{BoolQueryBuilder, GeQueryBuilder, GeQueryBuilders}
import com.mware.ge.values.storable.{Value, Values}
import io.bigconnect.spark.util.BcMapping.{CONCEPT_TYPE_FIELD, DEST_ID_FIELD, EDGE_LABEL_FIELD, ID_FIELD, SOURCE_ID_FIELD, convertToInternalRow}
import io.bigconnect.spark.util.{BcMapping, BcOptions, BcUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{And, EqualNullSafe, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable

class BasePartitionReaderByShard(private val options: BcOptions,
                                 private val shard: String,
                                 private val filters: Array[Filter],
                                 private val jobId: String,
                                 private val schema: StructType,
                                 private val requiredColumns: StructType) extends Logging {

  private val graph = BcUtil.createGraph(options.configuration.toBcConfig)
  private val queryBuilder: BoolQueryBuilder = boolQuery();

  filters.foreach(f => queryBuilder.and(applyFiltersToQuery(f)));
  private val query: Query = graph.query(queryBuilder, options.authorizations)
    .setShard(shard)

  val iterable: QueryResultsIterable[Element] = ElementType.parse(options.elementType) match {
    case ElementType.VERTEX => query.vertices(options.fetchHints).asInstanceOf[QueryResultsIterable[Element]]
    case ElementType.EDGE => query.edges(options.fetchHints).asInstanceOf[QueryResultsIterable[Element]]
  }
  val searchResults = iterable.iterator().asScala

  def next(): Boolean = {
    searchResults.hasNext
  }

  def get(): InternalRow = {
    val element = searchResults.next();
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
    if (searchResults != null)
      iterable.close()
  }

  def applyFiltersToQuery(filter: Filter): GeQueryBuilder = {
    filter match {
      case And(left, right) =>
        boolQuery()
          .and(applyFiltersToQuery(left))
          .and(applyFiltersToQuery(right))
      case Or(left, right) =>
        boolQuery()
          .or(applyFiltersToQuery(left))
          .or(applyFiltersToQuery(right))
      case Not(child) =>
        boolQuery()
          .andNot(applyFiltersToQuery(child))
      case IsNull(attribute) =>
        boolQuery().andNot(exists(attribute))
      case IsNotNull(attribute) =>
        GeQueryBuilders.exists(attribute)
      case EqualTo(attribute, value) =>
        attribute match {
          case BcMapping.ID_FIELD => hasIds(value.asInstanceOf[String])
          case BcMapping.CONCEPT_TYPE_FIELD => hasConceptType(value.asInstanceOf[String])
          case BcMapping.EDGE_LABEL_FIELD => hasEdgeLabel(value.asInstanceOf[String])
          case _ => hasFilter(attribute, Compare.EQUAL, Values.of(value))
        }
      case EqualNullSafe(attribute, value) =>
        if (value == null)
          exists(attribute)
        else
        hasFilter(attribute, Compare.EQUAL, Values.of(value))
      case GreaterThan(attribute, value) =>
        hasFilter(attribute, Compare.GREATER_THAN, Values.of(value))
      case GreaterThanOrEqual(attribute, value) =>
        hasFilter(attribute, Compare.GREATER_THAN_EQUAL, Values.of(value))
      case LessThan(attribute, value) =>
        hasFilter(attribute, Compare.LESS_THAN, Values.of(value))
      case LessThanOrEqual(attribute, value) =>
        hasFilter(attribute, Compare.LESS_THAN_EQUAL, Values.of(value))
      case In(attribute, values) => {
        // when dealing with mixed types (strings and numbers) Spark converts the Strings to null (gets confused by the type field)
        // this leads to incorrect query DSL hence why nulls are filtered
        val filtered = values filter (_ != null)
        if (filtered.isEmpty) {
          return searchAll()
        }
        hasFilter(attribute, Contains.IN, Values.of(filtered))
      }
      case StringContains(attribute, value) =>
        hasFilter(attribute, TextPredicate.CONTAINS, Values.stringValue(value))
      case StringStartsWith(attribute, value) =>
        hasFilter(attribute, Compare.STARTS_WITH, Values.stringValue(value))
      case StringEndsWith(attribute, value) =>
        hasFilter(attribute, Compare.ENDS_WITH, Values.stringValue(value))
    }
  }
}

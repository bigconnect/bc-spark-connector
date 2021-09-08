package io.bigconnect.spark.reader

import _root_.io.bigconnect.spark.util.BcMapping._
import _root_.io.bigconnect.spark.util.{BcMapping, BcOptions, BcUtil}
import com.mware.ge._
import com.mware.ge.query._
import com.mware.ge.query.builder.GeQueryBuilders._
import com.mware.ge.query.builder.{BoolQueryBuilder, GeQueryBuilder, GeQueryBuilders}
import com.mware.ge.values.storable.{DateValue, LocalDateTimeValue, Value, Values}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
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
          case _ => hasFilter(attribute, Compare.EQUAL, sparkValueToBcValue(attribute, value))
        }
      case EqualNullSafe(attribute, value) =>
        if (value == null)
          exists(attribute)
        else
          hasFilter(attribute, Compare.EQUAL, sparkValueToBcValue(attribute, value))
      case GreaterThan(attribute, value) =>
        hasFilter(attribute, Compare.GREATER_THAN, sparkValueToBcValue(attribute, value))
      case GreaterThanOrEqual(attribute, value) =>
        hasFilter(attribute, Compare.GREATER_THAN_EQUAL, sparkValueToBcValue(attribute, value))
      case LessThan(attribute, value) =>
        hasFilter(attribute, Compare.LESS_THAN, sparkValueToBcValue(attribute, value))
      case LessThanOrEqual(attribute, value) =>
        hasFilter(attribute, Compare.LESS_THAN_EQUAL, sparkValueToBcValue(attribute, value))
      case In(attribute, values) => {
        // when dealing with mixed types (strings and numbers) Spark converts the Strings to null (gets confused by the type field)
        // this leads to incorrect query DSL hence why nulls are filtered
        val filtered = values filter (_ != null)
        if (filtered.isEmpty) {
          return searchAll()
        }
        hasFilter(attribute, Contains.IN, sparkValueToBcValue(attribute, filtered))
      }
      case StringContains(attribute, value) =>
        hasFilter(attribute, TextPredicate.CONTAINS, Values.stringValue(value))
      case StringStartsWith(attribute, value) =>
        hasFilter(attribute, Compare.STARTS_WITH, Values.stringValue(value))
      case StringEndsWith(attribute, value) =>
        hasFilter(attribute, Compare.ENDS_WITH, Values.stringValue(value))
    }
  }

  def sparkValueToBcValue(field: String, value: Any): Value = {
    val isArray = value.isInstanceOf[Array[_]]
    schema.apply(field).dataType match {
      case BooleanType => if (isArray) Values.booleanArray(value.asInstanceOf[Array[_]].map(v => v.asInstanceOf[Boolean])) else Values.booleanValue(value.asInstanceOf[Boolean])
      case IntegerType => if (isArray) Values.intArray(value.asInstanceOf[Array[_]].map(v => v.asInstanceOf[Int])) else Values.intValue(value.asInstanceOf[Int])
      case LongType => if (isArray) Values.longArray(value.asInstanceOf[Array[_]].map(v => v.asInstanceOf[Long])) else Values.longValue(value.asInstanceOf[Long])
      case ShortType => if (isArray) Values.shortArray(value.asInstanceOf[Array[_]].map(v => v.asInstanceOf[Short])) else Values.shortValue(value.asInstanceOf[Short])
      case ByteType => if (isArray) Values.byteArray(value.asInstanceOf[Array[_]].map(v => v.asInstanceOf[Byte])) else Values.byteValue(value.asInstanceOf[Byte])
      case FloatType => if (isArray) Values.floatArray(value.asInstanceOf[Array[_]].map(v => v.asInstanceOf[Float])) else Values.floatValue(value.asInstanceOf[Float])
      case DoubleType => if (isArray) Values.doubleArray(value.asInstanceOf[Array[_]].map(v => v.asInstanceOf[Double])) else Values.doubleValue(value.asInstanceOf[Double])
      case DateType => if (isArray) Values.dateArray(value.asInstanceOf[Array[Date]].map(d => d.toLocalDate)) else DateValue.date(value.asInstanceOf[Date].toLocalDate)
      case TimestampType => if (isArray) Values.localDateTimeArray(value.asInstanceOf[Array[Timestamp]].map(d => d.toLocalDateTime)) else LocalDateTimeValue.localDateTime(value.asInstanceOf[Timestamp].toLocalDateTime)
      case StringType => if (isArray) Values.stringArray(value.asInstanceOf[Array[String]]: _*) else Values.stringValue(value.asInstanceOf[String])
      case BcMapping.durationType => throw new IllegalArgumentException("Duration filters not supported")
      case _ => Values.of(value)
    }
  }
}

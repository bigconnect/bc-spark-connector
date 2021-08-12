package io.bigconnect.spark.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.mware.core.config.Configuration
import com.mware.core.model.clientapi.dto.PropertyType
import com.mware.core.model.schema.SchemaRepository
import com.mware.ge.mutation.ElementMutation
import com.mware.ge.values.storable.{Value, Values}
import com.mware.ge.{Element, ElementType, Visibility}
import io.bigconnect.spark.util.BcUtil._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.InputStream
import java.time.ZoneOffset
import java.util.Optional
import scala.collection.JavaConverters._
import scala.collection.mutable

object BcMapping {
  val TYPE_VERTEX = "vertex"
  val TYPE_EDGE = "edge"

  val ID_FIELD = "_id"
  val SOURCE_ID_FIELD = "_sid"
  val DEST_ID_FIELD = "_did"
  val CONCEPT_TYPE_FIELD = "_conceptType"
  val EDGE_LABEL_FIELD = "_edgeLabel"

  val mapper = new ObjectMapper()

  def convertFromSpark(value: Any, schema: StructField = null): Value = {
    value match {
      case date: java.sql.Date => convertFromSpark(date.toLocalDate, schema)
      case timestamp: java.sql.Timestamp => convertFromSpark(timestamp.toInstant.atZone(ZoneOffset.UTC), schema)
      case intValue: Int if schema != null && schema.dataType == DataTypes.DateType => convertFromSpark(DateTimeUtils
        .toJavaDate(intValue), schema)
      case longValue: Long if schema != null && schema.dataType == DataTypes.TimestampType => convertFromSpark(DateTimeUtils
        .toJavaTimestamp(longValue), schema)
      case string: UTF8String => convertFromSpark(string.toString)
      case _ => Values.of(value)
    }
  }

  def convertFromInternalRow(row: InternalRow, schema: StructType, m: ElementMutation[_ <: Element]): ElementMutation[_ <: Element] = {
    val seq = row.toSeq(schema)

    (0 to schema.size - 1)
      .flatMap(i => {
        val field = schema(i)
        if (!isIgnoredProperty(field.name)) {
          val bcValue = BcMapping.convertFromSpark(seq(i), field)
          m.setProperty(field.name, bcValue, Visibility.EMPTY)
        }
        None
      })
    m
  }

  private def isIgnoredProperty(prop: String): Boolean = prop match {
    case CONCEPT_TYPE_FIELD => true
    case ID_FIELD => true
    case EDGE_LABEL_FIELD => true
    case SOURCE_ID_FIELD => true
    case DEST_ID_FIELD => true
    case _ => false
  }

  def convertToInternalRow(map: mutable.Map[String, Value], schema: StructType): InternalRow = InternalRow
    .fromSeq(
      schema.map(field => bcValueToSpark(map.get(field.name).map(v => v.asObjectCopy()).orNull, field.dataType))
    )

  def bcValueToSpark(value: Any, schema: DataType): Any = {
    if (schema != null && schema == DataTypes.StringType && value != null && !value.isInstanceOf[String]) {
      if (value.isInstanceOf[InputStream])
        bcValueToSpark("<>", schema)
      else
        bcValueToSpark(mapper.writeValueAsString(value), schema)
    } else {
      value match {
        case s: String => UTF8String.fromString(s)
        case _ => value
      }
    }
  }

  def structFromSchema(config: Configuration, elementType: ElementType): StructType = {
    schemaRepository = createSchemaRepository(createGraph(config), config)
    val structFields: mutable.Buffer[StructField] = schemaRepository.getProperties(SchemaRepository.PUBLIC)
      .asScala
      .filter(p => Option.apply(p.getUserVisible).getOrElse(false))
      .map(p => {
        StructField(p.getName, toSparkDataType(p.getDataType))
      })
      .toBuffer
      .sortBy(t => t.name)

    structFields += StructField(ID_FIELD, DataTypes.StringType, nullable = false)
    if (elementType == ElementType.VERTEX) {
      structFields += StructField(CONCEPT_TYPE_FIELD, DataTypes.StringType, nullable = false)
    } else if (elementType == ElementType.EDGE) {
      structFields += StructField(EDGE_LABEL_FIELD, DataTypes.StringType, nullable = false)
      structFields += StructField(SOURCE_ID_FIELD, DataTypes.StringType, nullable = false)
      structFields += StructField(DEST_ID_FIELD, DataTypes.StringType, nullable = false)
    }
    StructType(structFields.reverse)
  }

  def toSparkDataType(bcDataType: PropertyType): DataType = {
    bcDataType match {
      case PropertyType.DATE => DataTypes.DateType
      case PropertyType.DOUBLE => DataTypes.DoubleType
      case PropertyType.BOOLEAN => DataTypes.BooleanType
      case PropertyType.INTEGER => DataTypes.IntegerType
      // Default is String
      case _ => DataTypes.StringType
    }
  }

  def getColValue(row: InternalRow, schema: StructType, col: String): Optional[Value] = {
    try {
      val idx = schema.fieldIndex(col)
      Optional.of(convertFromSpark(row.toSeq(schema)(idx), schema(idx)))
    } catch {
      case _: Throwable => Optional.empty()
    }
  }
}

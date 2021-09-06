package io.bigconnect.spark.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.mware.core.config.Configuration
import com.mware.core.model.clientapi.dto.PropertyType
import com.mware.core.model.schema.SchemaRepository
import com.mware.ge.mutation.ElementMutation
import com.mware.ge.values.storable._
import com.mware.ge.{Element, ElementType, Visibility}
import io.bigconnect.spark.util.BcUtil._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.Optional
import scala.collection.JavaConverters._
import scala.collection.mutable

object BcMapping {
  val TYPE_VERTEX = "VERTEX"
  val TYPE_EDGE = "EDGE"

  val ID_FIELD = "_id"
  val SOURCE_ID_FIELD = "_sid"
  val DEST_ID_FIELD = "_did"
  val CONCEPT_TYPE_FIELD = "_conceptType"
  val EDGE_LABEL_FIELD = "_edgeLabel"

  val POINT_TYPE = "point"
  val DURATION_TYPE = "duration"

  val mapper = new ObjectMapper()

  val durationType: DataType = DataTypes.createStructType(Array(
    DataTypes.createStructField("type", DataTypes.StringType, false),
    DataTypes.createStructField("months", DataTypes.LongType, false),
    DataTypes.createStructField("days", DataTypes.LongType, false),
    DataTypes.createStructField("seconds", DataTypes.LongType, false),
    DataTypes.createStructField("nanoseconds", DataTypes.IntegerType, false),
    DataTypes.createStructField("value", DataTypes.StringType, false)
  ))

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
      schema.map(field => bcValueToSpark(map.get(field.name).orNull, field.dataType))
    )

  def bcValueToSpark(value: Any, schema: DataType): Any = {
    value match {
      case zt: DateTimeValue => DateTimeUtils.instantToMicros(zt.asObjectCopy().toInstant)
      case dt: LocalDateTimeValue => DateTimeUtils.instantToMicros(dt.asObjectCopy().toInstant(ZoneOffset.UTC))
      case d: DateValue => d.asObjectCopy().toEpochDay.toInt
      case s: TextValue => UTF8String.fromString(s.stringValue())
      case dv: DurationValue => {
        val months = dv.get(ChronoUnit.MONTHS)
        val days = dv.get(ChronoUnit.DAYS)
        val seconds = dv.get(ChronoUnit.SECONDS)
        val nanoseconds: Integer = dv.get(ChronoUnit.NANOS).toInt
        InternalRow.fromSeq(Seq(
          UTF8String.fromString(DURATION_TYPE), months, days, seconds, nanoseconds, UTF8String.fromString(dv.toString))
        )
      }
      case a: ArrayValue => {
        val elementType = if (schema != null) schema.asInstanceOf[ArrayType].elementType else null
        ArrayData.toArrayData(a.asScala.map(e => bcValueToSpark(e, elementType)).toArray)
      }
      case v: Value => v.asObjectCopy()
      case _ => value
    }
  }

  def structFromSchema(config: Configuration, elementType: ElementType): StructType = {
    schemaRepository = createSchemaRepository(createGraph(config), config)
    val structFields: mutable.Buffer[StructField] = schemaRepository.getProperties(SchemaRepository.PUBLIC)
      .asScala
      .filter(p => Option.apply(p.getUserVisible).getOrElse(false))
      .map(p => {
        StructField(p.getName, toSparkDataType(p.getName, p.getDataType))
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

  def toSparkDataType(name: String, bcDataType: PropertyType): DataType = {
    bcDataType match {
      case PropertyType.BOOLEAN => DataTypes.BooleanType
      case PropertyType.BOOLEAN_ARRAY => DataTypes.createArrayType(DataTypes.BooleanType)
      case PropertyType.INTEGER => DataTypes.IntegerType
      case PropertyType.INTEGER_ARRAY => DataTypes.createArrayType(DataTypes.IntegerType)
      case PropertyType.LONG => DataTypes.LongType
      case PropertyType.LONG_ARRAY => DataTypes.createArrayType(DataTypes.LongType)
      case PropertyType.SHORT => DataTypes.ShortType
      case PropertyType.SHORT_ARRAY => DataTypes.createArrayType(DataTypes.ShortType)
      case PropertyType.BYTE => DataTypes.ByteType
      case PropertyType.BYTE_ARRAY => DataTypes.createArrayType(DataTypes.ByteType)
      case PropertyType.FLOAT => DataTypes.FloatType
      case PropertyType.FLOAT_ARRAY => DataTypes.createArrayType(DataTypes.FloatType)
      case PropertyType.DOUBLE => DataTypes.DoubleType
      case PropertyType.DOUBLE_ARRAY => DataTypes.createArrayType(DataTypes.DoubleType)
      case PropertyType.DATE => DataTypes.TimestampType
      case PropertyType.DATETIME => DataTypes.TimestampType
      case PropertyType.DATETIME_ARRAY => DataTypes.createArrayType(DataTypes.TimestampType)
      case PropertyType.LOCAL_DATE => DataTypes.DateType
      case PropertyType.LOCAL_DATE_ARRAY => DataTypes.createArrayType(DataTypes.DateType)
      case PropertyType.LOCAL_DATETIME => DataTypes.TimestampType
      case PropertyType.LOCAL_DATETIME_ARRAY => DataTypes.createArrayType(DataTypes.TimestampType)
      case PropertyType.STREAMING => DataTypes.BinaryType
      case PropertyType.STRING_ARRAY => DataTypes.createArrayType(DataTypes.StringType)
      case PropertyType.DURATION => durationType
      case PropertyType.DURATION_ARRAY => DataTypes.createArrayType(durationType)
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

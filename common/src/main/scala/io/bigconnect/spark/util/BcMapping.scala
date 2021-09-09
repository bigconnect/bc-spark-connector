package io.bigconnect.spark.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.mware.core.config.Configuration
import com.mware.core.model.clientapi.dto.PropertyType
import com.mware.core.model.properties.SchemaProperties
import com.mware.core.model.schema.{SchemaFactory, SchemaRepository}
import com.mware.ge.mutation.ElementMutation
import com.mware.ge.values.storable._
import com.mware.ge.{Element, ElementType, TextIndexHint, Visibility}
import io.bigconnect.spark.util.BcUtil._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, UnsafeArrayData, UnsafeRow}
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

  def ensureElementTypeExists(options: BcOptions): Unit = {
    val bcConfig = options.configuration.toBcConfig
    val schemaRepository = createSchemaRepository(createGraph(bcConfig), bcConfig)
    val schemaFactory = new SchemaFactory(schemaRepository).forNamespace(SchemaRepository.PUBLIC)
    val thingConcept = schemaFactory.getOrCreateThingConcept()
    options.elementType match {
      case BcMapping.TYPE_VERTEX => {
        val existingConcept = schemaRepository.getConceptByName(options.conceptType)
        if (existingConcept == null)
          schemaFactory.newConcept()
            .parent(thingConcept)
            .conceptType(options.conceptType)
            .displayName(options.conceptType)
            .property(SchemaProperties.USER_VISIBLE.getPropertyName, BooleanValue.TRUE)
            .save
      }
      case BcMapping.TYPE_EDGE => {
        val existingRel = schemaRepository.getRequiredConceptByName(options.labelType)
        if (existingRel == null)
          schemaFactory.newRelationship()
            .parent(schemaFactory.getOrCreateRootRelationship())
            .label(options.labelType)
            .source(thingConcept)
            .target(thingConcept)
            .property(SchemaProperties.USER_VISIBLE.getPropertyName, BooleanValue.TRUE)
            .save
      }
    }
  }

  def convertFromSpark(value: Any, schema: StructField = null): Value = {
    value match {
      case date: java.sql.Date => Values.of(date.toLocalDate)
      case timestamp: java.sql.Timestamp => Values.of(timestamp.toInstant.atZone(ZoneOffset.UTC))
      case intValue: Int if schema != null && schema.dataType == DataTypes.DateType => convertFromSpark(DateTimeUtils.toJavaDate(intValue))
      case longValue: Long if schema != null && schema.dataType == DataTypes.TimestampType => convertFromSpark(DateTimeUtils.toJavaTimestamp(longValue))
      case unsafeRow: UnsafeRow => {
        val structType = extractStructType(schema.dataType)
        val row = new GenericRowWithSchema(unsafeRow.toSeq(structType).toArray, structType)
        convertFromSpark(row)
      }
      case struct: GenericRowWithSchema => {
        val typ = struct.getAs[UTF8String]("type").toString
        typ match {
          case DURATION_TYPE => DurationValue.duration(
            struct.getAs[Number]("months").longValue(),
            struct.getAs[Number]("days").longValue(),
            struct.getAs[Number]("seconds").longValue(),
            struct.getAs[Number]("nanoseconds").intValue())
          case _ => throw new IllegalArgumentException(s"Don't know how to convert a ${typ} to a BigConnect value")
        }
      }
      case unsafeArray: UnsafeArrayData => {
        val sparkType = schema.dataType match {
          case arrayType: ArrayType => arrayType.elementType
          case _ => schema.dataType
        }
        val safeArray = unsafeArray.toSeq[AnyRef](sparkType)
          .map(elem => convertFromSpark(elem, schema))
          .map(v => v.asObjectCopy())
          .toArray

        Values.of(safeArray)
      }
      case string: UTF8String => Values.stringValue(string.toString)
      case _ => Values.of(value)
    }
  }

  private def extractStructType(dataType: DataType): StructType = dataType match {
    case structType: StructType => structType
    case mapType: MapType => extractStructType(mapType.valueType)
    case arrayType: ArrayType => extractStructType(arrayType.elementType)
    case _ => throw new UnsupportedOperationException(s"$dataType not supported")
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

  def ensureSchemaCreated(config: Configuration, sparkSchema: StructType) = {
    schemaRepository = createSchemaRepository(createGraph(config), config)
    val schemaProps = schemaRepository.getProperties(SchemaRepository.PUBLIC).asScala
      .map(p => p.getName).toSeq

    val propsToAdd = sparkSchema.map(f => f.name).diff(schemaProps)

    if (propsToAdd.nonEmpty) {
      val schemaFactory = new SchemaFactory(schemaRepository).forNamespace(SchemaRepository.PUBLIC)

      sparkSchema.filter(f => propsToAdd.contains(f.name))
        .foreach(f => {
          val bcType = toBcDataType(f.dataType)
          val prop = schemaFactory.newConceptProperty
            .name(f.name)
            .concepts(schemaRepository.getThingConcept)
            .displayName(f.name)
            .`type`(bcType)
            .systemProperty(false)
            .userVisible(true)
            .searchable(true)
            .updatable(true)
            .addable(true)

          if (f.dataType == StringType) {
            prop.textIndexHints(TextIndexHint.ALL)
          } else {
            prop.textIndexHints(TextIndexHint.EXACT_MATCH)
          }

          prop.save()
        })

      schemaRepository.clearCache()
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

  def toBcDataType(sparkType: DataType): PropertyType = {
    var isArray = false
    var typeToTest = sparkType
    if (sparkType.isInstanceOf[ArrayType]) {
      isArray = true
      typeToTest = sparkType.asInstanceOf[ArrayType].elementType
    }
    typeToTest match {
      case DataTypes.BooleanType => if (isArray) PropertyType.BOOLEAN_ARRAY else PropertyType.BOOLEAN
      case DataTypes.IntegerType => if (isArray) PropertyType.INTEGER_ARRAY else PropertyType.INTEGER
      case DataTypes.LongType => if (isArray) PropertyType.LONG_ARRAY else PropertyType.LONG
      case DataTypes.ShortType => if (isArray) PropertyType.SHORT_ARRAY else PropertyType.SHORT
      case DataTypes.ByteType => if (isArray) PropertyType.BYTE_ARRAY else PropertyType.BYTE
      case DataTypes.FloatType => if (isArray) PropertyType.FLOAT_ARRAY else PropertyType.FLOAT
      case DataTypes.DoubleType => if (isArray) PropertyType.DOUBLE_ARRAY else PropertyType.DOUBLE
      case DataTypes.TimestampType => if (isArray) PropertyType.LOCAL_DATETIME_ARRAY else PropertyType.LOCAL_DATETIME
      case DataTypes.DateType => if (isArray) PropertyType.LOCAL_DATE_ARRAY else PropertyType.LOCAL_DATE
      // default is STRING
      case _ => if (isArray) PropertyType.STRING_ARRAY else PropertyType.STRING
    }
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

package io.bigconnect.spark3

import com.mware.ge.ElementType
import io.bigconnect.spark.util.{BcMapping, BcOptions, Validations}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.UUID

class DataSource extends TableProvider with DataSourceRegister {
  Validations.version("3.*")
  private val jobId: String = UUID.randomUUID().toString
  private var schema: StructType = null
  private var bcOptions: BcOptions = null

  override def supportsExternalMetadata(): Boolean = true

  override def shortName(): String = "bigconnect"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (schema == null) {
      val bcOpts = getBcOptions(options)
      schema = BcMapping.structFromSchema(bcOpts.configuration.toBcConfig, ElementType.parse(bcOptions.elementType))
    }
    schema
  }

  private def getBcOptions(caseInsensitiveStringMap: CaseInsensitiveStringMap) = {
    if(bcOptions == null) {
      bcOptions = new BcOptions(caseInsensitiveStringMap.asCaseSensitiveMap())
    }

    bcOptions
  }

  override def getTable(structType: StructType, partitioning: Array[Transform], map: util.Map[String, String]): Table = {
    val caseInsensitiveStringMapBcOptions = new CaseInsensitiveStringMap(map)
    val schema = if (structType != null) {
      structType
    } else {
      inferSchema(caseInsensitiveStringMapBcOptions)
    }
    new BcTable(schema, map, jobId)
  }

}

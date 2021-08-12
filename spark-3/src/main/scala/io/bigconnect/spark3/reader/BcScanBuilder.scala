package io.bigconnect.spark3.reader

import io.bigconnect.spark.util.BcOptions
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class BcScanBuilder(bcOptions: BcOptions, jobId: String, schema: StructType) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private var filters: Array[Filter] = Array[Filter]()

  private var requiredColumns: StructType = new StructType()

  override def build(): Scan = {
    new BcScan(bcOptions, jobId, schema, filters, requiredColumns)
  }

  override def pushFilters(filtersArray: Array[Filter]): Array[Filter] = {
    filters = filtersArray
    filtersArray
  }

  override def pushedFilters(): Array[Filter] = filters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    requiredColumns = if (requiredSchema == schema) {
      new StructType()
    } else {
      requiredSchema
    }
  }
}

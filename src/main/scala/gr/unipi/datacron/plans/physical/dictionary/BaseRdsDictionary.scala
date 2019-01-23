package gr.unipi.datacron.plans.physical.dictionary

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

abstract private[dictionary] class BaseRdsDictionary extends BasePhysicalPlan with TDictionary {
  override def decodeSingleKey(params: decodeSingleKeyParams): Option[String] = DataStore.dictionaryRedis.getDecodedValue(params.key)

  override def encodeSingleValue(params: encodeSingleValueParams): Option[Long] = DataStore.dictionaryRedis.getEncodedValue(params.value)

  protected def decodeColumn(df: DataFrame, columnName: String, newColumnName: String): DataFrame

  override def decodeColumn(params: decodeColumnParams): DataFrame = {
    val newColName = if (params.preserveColumnName) params.columnName else params.columnName + tripleTranslateSuffix
    decodeColumn(params.df, params.columnName, newColName)
  }

  override def decodeColumns(params: decodeColumnsParams): DataFrame = {
    var result = params.df
    params.columnNames.foreach(c => {
      val newColName = if (params.preserveColumnNames) c else c + tripleTranslateSuffix
      result = decodeColumn(result, c, newColName)
    })

    result
  }

  override def decodeAllColumns(params: decodeAllColumnsParams): DataFrame =
    decodeColumns(decodeColumnsParams(params.df, params.df.columns, preserveColumnNames = true))


  override def decodeAllColumnsExceptFor(params: decodeAllColumnsExceptForParams): DataFrame =
    decodeColumns(decodeColumnsParams(params.df, params.df.columns.filterNot(params.exceptForColumnNames.contains), preserveColumnNames = true))
}

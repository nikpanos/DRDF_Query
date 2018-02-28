package gr.unipi.datacron.plans.physical.dictionary

import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame
import gr.unipi.datacron.common.Consts._

abstract private[dictionary] class BaseRdsDictionary extends BasePhysicalPlan with TDictionary {
  override def pointSearchValue(params: pointSearchValueParams): Option[String] = DataStore.dictionaryRedis.getDecodedValue(params.key)

  override def pointSearchKey(params: pointSearchKeyParams): Option[Long] = DataStore.dictionaryRedis.getEncodedValue(params.value)

  protected def decodeColumn(df: DataFrame, columnName: String, newColumnName: String): DataFrame

  override def decodeColumn(params: decodeColumnParams): DataFrame =
    decodeColumn(params.dfTriples, params.columnName, params.columnName + tripleTranslateSuffix)

  override def decodeColumns(params: decodeColumnsParams): DataFrame = {
    var result = params.dfTriples
    params.columnNames.foreach(c => {
      result = decodeColumn(result, c, c + tripleTranslateSuffix)
    })

    result
  }
}

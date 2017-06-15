package gr.unipi.datacron.plans.physical.dictionary

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.Consts._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.DataFrame

case class RdsDictionary() extends BasePhysicalPlan with TDictionary {
  override def pointSearchValue(params: pointSearchValueParams): Option[String] = DataStore.dictionaryRedis.getDecodedValue(params.key)

  override def pointSearchKey(params: pointSearchKeyParams): Option[Long] = DataStore.dictionaryRedis.getEncodedValue(params.value)

    private def decodeColumn(df: DataFrame, columnName: String, newColumnName: String): DataFrame = {
      val bCnf = DataStore.bConfig

      val translate: UserDefinedFunction = udf((field: Long) => {
        if (!AppConfig.isAssigned) {
          AppConfig.setConfig(bCnf.value)
        }
        DataStore.dictionaryRedis.getDecodedValue(field)
      })
      df.withColumn(newColumnName, translate(col(columnName)))
    }

  override def translateColumn(params: translateColumnParams): DataFrame =
    decodeColumn(params.dfTriples, params.columnName, params.columnName + tripleTranslateSuffix)

  override def translateColumns(params: translateColumnsParams): DataFrame = {
    var result = params.dfTriples
    params.columnNames.foreach(c => {
      result = decodeColumn(result, c, c + tripleTranslateSuffix)
    })

    result
  }
}

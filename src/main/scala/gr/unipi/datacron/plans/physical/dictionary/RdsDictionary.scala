package gr.unipi.datacron.plans.physical.dictionary

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits.TDictionary
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.Consts._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.DataFrame

case class RdsDictionary() extends BasePhysicalPlan with TDictionary {
  override def pointSearchValue(key: Long): Option[String] = DataStore.dictionaryRedis.getDecodedValue(key)

  override def pointSearchKey(value: String): Option[Long] = DataStore.dictionaryRedis.getEncodedValue(value)

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

  override def translateColumn(dfTriples: DataFrame, columnName: String): DataFrame =
    decodeColumn(dfTriples, columnName, columnName + tripleTranslateSuffix)

  override def translateColumns(dfTriples: DataFrame, columnNames: Array[String]): DataFrame = {
    var result = dfTriples
    columnNames.foreach(c => {
      result = decodeColumn(result, c, c + tripleTranslateSuffix)
    })

    result
  }
}

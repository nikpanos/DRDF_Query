package gr.unipi.datacron.plans.physical.dictionary

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.DataFrame
import gr.unipi.datacron.common.DataFrameUtils._

case class RdsDictionary() extends BaseRdsDictionary {

  protected override def decodeColumn(df: DataFrame, columnName: String, newColumnName: String): DataFrame = {
    val bCnf = DataStore.bConfig

    val translate: UserDefinedFunction = udf((field: Long) => {
      if (!AppConfig.isAssigned) {
        AppConfig.setConfig(bCnf.value)
      }
      DataStore.dictionaryRedis.getDecodedValue(field)
    })
    df.withColumn(newColumnName, translate(df(sanitize(columnName))))
  }
}

package gr.unipi.datacron.plans.physical.dictionary

import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits.TDictionary
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.Consts._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

/**
  * Created by nikp on 6/7/17.
  */
case class RdsDictionary() extends BasePhysicalPlan with TDictionary {
  override def pointSearchValue(key: Long): Option[String] = DataStore.dictionaryRedis.getDecodedValue(key)

  override def pointSearchKey(value: String): Option[Long] = DataStore.dictionaryRedis.getEncodedValue(value)

  private val translate: UserDefinedFunction = udf((field: Long) => DataStore.dictionaryRedis.getDecodedValue(field))

  override def translateColumn(dfTriples: DataFrame, columnName: String): DataFrame =
    dfTriples.withColumn(columnName + tripleTranslateSuffix, translate(col(columnName)))

  override def translateColumns(dfTriples: DataFrame, columnNames: Array[String]): DataFrame = {
    var result = dfTriples
    columnNames.foreach(c => {
      result = result.withColumn(c + tripleTranslateSuffix, translate(col(c)))

      //DataStore.doWithConfig()
    })


    result
  }
}

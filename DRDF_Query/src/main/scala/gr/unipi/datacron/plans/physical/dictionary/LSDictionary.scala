package gr.unipi.datacron.plans.physical.dictionary

import gr.unipi.datacron.plans.physical.traits.TDictionary
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.Try

case class LSDictionary() extends BasePhysicalPlan with TDictionary {
  import DataStore.spark.implicits._
  
  def pointSearchValue(df: DataFrame, key: Long): Option[String] =
    Try(df.filter(col(dicKeyLongField) === key).first().getAs[String](dicValueStrField)).toOption
  
  def pointSearchKey(df: DataFrame, value: String): Option[Long] =
    Try(df.filter(col(dicValueStrField) === value).first().getAs[Long](dicKeyLongField)).toOption

  def translateColumn(dfTriples: DataFrame, dfDictionary: DataFrame, columnName: String): DataFrame = {
    val collected = dfTriples.select(columnName).as[Long].collect.toSet
    val bCollected = DataStore.sc.broadcast(collected)

    val contains = udf((key: Long) => bCollected.value.contains(key))

    val dicMap = dfDictionary.filter(contains(col(dicKeyLongField))).as[(Long, String)].collect.toMap
    val bDicMap = DataStore.sc.broadcast(dicMap)

    val translate = udf((field: Long) => bDicMap.value(field))

    dfTriples.withColumn(columnName + tripleTranslateSuffix, translate(col(columnName)))
  }

  def translateColumns(dfTriples: DataFrame, dfDictionary: DataFrame, columnNames: Array[String]): DataFrame = {
    val bColumns = DataStore.sc.broadcast(columnNames)

    val collected = dfTriples.rdd.flatMap(row => {
      bColumns.value.map(x => row.getAs[Long](x))
    }).collect.toSet
    val bCollected = DataStore.sc.broadcast(collected)

    val contains = udf((key: Long) => bCollected.value.contains(key))

    val dicMap = dfDictionary.filter(contains(col(dicKeyLongField))).as[(Long, String)].collect.toMap
    val bDicMap = DataStore.sc.broadcast(dicMap)

    val translate = udf((field: Long) => bDicMap.value(field))

    var result = dfTriples
    columnNames.foreach(c => {
      result = result.withColumn(c + tripleTranslateSuffix, translate(col(c)))
    })
    result
  }
}
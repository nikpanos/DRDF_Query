package gr.unipi.datacron.plans.physical.dictionary

import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.Try

case class LSDictionary() extends BasePhysicalPlan with TDictionary {
  import DataStore.spark.implicits._
  
  def pointSearchValue(params: pointSearchValueParams): Option[String] =
    Try(DataStore.dictionaryData.filter(col(dicKeyLongField) === params.key).first().getAs[String](dicValueStrField)).toOption
  
  def pointSearchKey(params: pointSearchKeyParams): Option[Long] =
    Try(DataStore.dictionaryData.filter(col(dicValueStrField) === params.value).first().getAs[Long](dicKeyLongField)).toOption

  def decodeColumn(params: decodeColumnParams): DataFrame = {
    val collected = params.dfTriples.select(params.columnName).as[Long].collect.toSet
    val bCollected = DataStore.sc.broadcast(collected)

    val contains = udf((key: Long) => bCollected.value.contains(key))

    val dicMap = DataStore.dictionaryData.filter(contains(col(dicKeyLongField))).as[(Long, String)].collect.toMap
    val bDicMap = DataStore.sc.broadcast(dicMap)

    val translate = udf((field: Long) => bDicMap.value(field))

    params.dfTriples.withColumn(params.columnName + tripleTranslateSuffix, translate(col(params.columnName)))
  }

  def decodeColumns(params: decodeColumnsParams): DataFrame = {
    val bColumns = DataStore.sc.broadcast(params.columnNames)

    val collected = params.dfTriples.rdd.flatMap(row => {
      bColumns.value.map(x => row.getAs[Long](x))
    }).collect.toSet
    val bCollected = DataStore.sc.broadcast(collected)

    val contains = udf((key: Long) => bCollected.value.contains(key))

    val dicMap = DataStore.dictionaryData.filter(contains(col(dicKeyLongField))).as[(Long, String)].collect.toMap
    val bDicMap = DataStore.sc.broadcast(dicMap)

    val translate = udf((field: Long) => bDicMap.value(field))

    var result = params.dfTriples
    params.columnNames.foreach(c => {
      result = result.withColumn(c + tripleTranslateSuffix, translate(col(c)))
    })
    result
  }
}
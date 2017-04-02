package gr.unipi.datacron.operators.joinDictionaryTriples

import gr.unipi.datacron.operators.BaseOperator
import gr.unipi.datacron.operators.traits.TJoinDictionaryTriples
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

case class MBJoinLSDictionarySTriples() extends BaseOperator with TJoinDictionaryTriples {
  import DataStore.spark.implicits._
  
  def translateColumn(dfTriples: DataFrame, dfDictionary: DataFrame, columnName: String): DataFrame = {
    val collected = dfTriples.select(columnName).as[Long].collect.toSet
    val bCollected = DataStore.sc.broadcast(collected)
    
    val contains = udf((key: Long) => bCollected.value.contains(key))
    
    val dicMap = dfDictionary.filter(contains(col(dicKeyLongField))).as[(Long, String)].collect.toMap
    val bDicMap = DataStore.sc.broadcast(dicMap)
    
    val translate = udf((field: Long) => {
      bDicMap.value.get(field).get
    })
    
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
    
    val translate = udf((field: Long) => {
      bDicMap.value.get(field).get
    })
    
    var result = dfTriples
    columnNames.foreach(c => {
      result = result.withColumn(c + tripleTranslateSuffix, translate(col(c)))
    })
    result
  }
}



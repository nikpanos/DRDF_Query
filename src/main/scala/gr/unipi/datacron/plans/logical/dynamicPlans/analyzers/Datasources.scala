package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.common.Consts.{nodeTypes, qfpDicRedisDynamicDatabaseID, weatherConditionTypes}
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.DatasourceOperator
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.store.DataStore._
import org.apache.spark.sql.DataFrame

object Datasources {
  private val nodeTypesEncoded: Set[Long] = convertDecodedArrayToEncodedSet(nodeTypes)
  private val weatherConditionTypesEncoded: Set[Long] = convertDecodedArrayToEncodedSet(weatherConditionTypes)

  private val propertyData: Array[DataFrame] = if ((AppConfig.getInt(qfpDicRedisDynamicDatabaseID) == 0) || (AppConfig.getInt(qfpDicRedisDynamicDatabaseID) == 2)) Array(nodeData, vesselData)
      else if ((AppConfig.getInt(qfpDicRedisDynamicDatabaseID) == 1) || (AppConfig.getInt(qfpDicRedisDynamicDatabaseID) == 3) || (AppConfig.getInt(qfpDicRedisDynamicDatabaseID) == 4)) Array(nodeData)
      else Array.empty[DataFrame]
  private val allData: Array[DataFrame] = propertyData :+ triplesData

  //private val datasources: Array[DatasourceOperator] = allData.map(DatasourceOperator)

  private def convertDecodedArrayToEncodedSet(arr: Array[String]): Set[Long] = arr.map(dictionaryRedis.getEncodedValue(_).get).toSet

  private def isPropertyTable(df: DataFrame): Boolean = propertyData.contains(df)

  private def createDatasource(df: DataFrame): DatasourceOperator = DatasourceOperator(df, isPropertyTable(df))

  def findDatasourceBasedOnRdfType(encodedRdfType: String): Array[DatasourceOperator] = {
    val encodedRdfTypeL = encodedRdfType.toLong
    if (nodeTypesEncoded.contains(encodedRdfTypeL)) Array(createDatasource(nodeData))
    else if (weatherConditionTypesEncoded.contains(encodedRdfTypeL)) Array(createDatasource(triplesData))
    else if ((AppConfig.getInt(qfpDicRedisDynamicDatabaseID) == 0) || (AppConfig.getInt(qfpDicRedisDynamicDatabaseID) == 2)) Array(createDatasource(vesselData), createDatasource(triplesData))
    else Array(createDatasource(triplesData))
  }

  def getDatasourcesByPredicates(predicates: Array[String]): Array[DatasourceOperator] = {
    propertyData.filter(df => {
      df.getIncludingColumns(predicates).length > 0
    }).map(createDatasource) :+ createDatasource(triplesData)
  }

  def getAllDatasourcesByIncludingAndExcludingPredicates(predicates: Array[String]): Array[DatasourceOperator] = {
    val result = propertyData.filter(df => {
      df.getIncludingColumns(predicates).length > 0
    })

    if (result.length == 0) {
      Array(createDatasource(triplesData))
    }
    else if (result.length == 1) {
      val df = result(0)
      val excl = df.getExcludingColumns(predicates)
      if (excl.length > 0) {
        Array(createDatasource(df), createDatasource(triplesData))
      }
      else {
        Array(createDatasource(df))
      }
    }
    else {
      throw new Exception("Does not support more than one datasources")
    }
  }
}

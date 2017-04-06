package gr.unipi.datacron.plans.physical.dictionary

import gr.unipi.datacron.plans.physical.traits.TDictionary
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import org.apache.spark.sql.DataFrame

case class SDictionary() extends BasePhysicalPlan with TDictionary {
  import DataStore.spark.implicits._
  
  def pointSearchValue(df: DataFrame, key: Long): Option[String] = {
    val searchStr = "^" + key + dicFieldsSeparator + ".*"
    try {
      val resultLine = df.filter(df(dicLineStrField).rlike(searchStr)).first().getAs[String](dicLineStrField)
      Some(resultLine.substring(resultLine.indexOf(dicFieldsSeparator) + 1, resultLine.length))
    }
    catch {
      case e: NoSuchElementException => None
    }
  }
  
  def pointSearchKey(df: DataFrame, value: String): Option[Long] = {
    val searchStr = "^-?\\d+" + dicFieldsSeparator + value
    try {
      val resultLine = df.filter(df(dicLineStrField).rlike(searchStr)).first().getAs[String](dicLineStrField)
      Some(resultLine.substring(0, resultLine.indexOf(dicFieldsSeparator)).toLong)
    }
    catch {
      case e: NoSuchElementException => None
    }
  }

  override def translateColumn(dfTriples: DataFrame, dfDictionary: DataFrame, columnName: String): DataFrame = {
    throw new NotImplementedError()
    //TODO: add implementation
  }

  override def translateColumns(dfTriples: DataFrame, dfDictionary: DataFrame, columnNames: Array[String]): DataFrame = {
    throw new NotImplementedError()
    //TODO: add implementation
  }
}
package gr.unipi.datacron.operators.dictionary

import gr.unipi.datacron.operators.BaseOperator
import gr.unipi.datacron.operators.traits.TDictionary
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.RegexUtils._
import org.apache.spark.sql.DataFrame

case class SDictionary() extends BaseOperator with TDictionary {
  import DataStore.spark.implicits._
  
  def pointSearchValue(df: DataFrame, key: Long): Option[String] = {
    val searchStr = "^" + key + dicFieldsSeparator + ".*"
    try {
      val resultLine = df.filter(df(dicLineStrField).rlike(searchStr)).first().getAs[String](dicLineStrField)
      return Some(resultLine.substring(resultLine.indexOf(dicFieldsSeparator), resultLine.length))
      //TODO: check that above statement yields correct result
    }
    catch {
      case e: Exception => return None  //TODO: fix the exception type here
    }
  }
  
  def pointSearchKey(df: DataFrame, value: String): Option[Long] = {
    val searchStr = ("^-?\\d+" + dicFieldsSeparator +  value)
    try {
      val resultLine = df.filter(df(dicLineStrField).rlike(searchStr)).first().getAs[String](dicLineStrField)
      return Some(resultLine.substring(0, resultLine.indexOf(dicFieldsSeparator)).toLong)
      //TODO: check that above statement yields correct result
    }
    catch {
      case e: Exception => return None  //TODO: fix the exception type here
    }
  }
}
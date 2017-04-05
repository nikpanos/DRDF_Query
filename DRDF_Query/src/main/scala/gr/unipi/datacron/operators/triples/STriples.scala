package gr.unipi.datacron.operators.triples

import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf

case class STriples() extends BaseTriples {
  import DataStore.spark.implicits._
  
  def filterBySPO(df: DataFrame, sub: Option[Long], pred: Option[Long], obj: Option[Long]): DataFrame = {
    var searchStr: String = ""
    
    searchStr += sub.getOrElse("^-?\\d+") + Consts.tripleFieldsSeparator
    searchStr += pred.getOrElse("-?\\d+") + Consts.tripleFieldsSeparator
    searchStr += obj.getOrElse("-?\\d+")
    
    df.filter(df(tripleSpoStrField) rlike searchStr)
  }
  
  def pointSearchObject(df: DataFrame, sub: Long, pred: Long): Option[Long] = {
    val sp = sub + tripleFieldsSeparator + pred
    val searchStr = sp + tripleFieldsSeparator + "-?\\d+"
    try {
      val resStr = df.filter(df(tripleSpoStrField) rlike searchStr).first.getAs[String](tripleSpoStrField)
      Some(resStr.substring(resStr.indexOf(tripleFieldsSeparator) + 1).toLong)
    }
    catch {
      case ex: NoSuchElementException => None
    }
  }
}



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

  private[triples] override def addSubjectInfo(df: DataFrame): DataFrame = {
    val getSubject = udf((spo: String) => {
      spo.substring(0, spo.indexOf(tripleFieldsSeparator)).toLong
    })

    df.withColumn(tripleSubLongField, getSubject(col(tripleSpoStrField)))
  }

  private[triples] override def addPredicateInfo(df: DataFrame): DataFrame = {
    val getPredicate = udf((spo: String) => {
      spo.substring(spo.indexOf(tripleFieldsSeparator) + 1, spo.lastIndexOf(tripleFieldsSeparator)).toLong
    })

    df.withColumn(triplePredLongField, getPredicate(col(tripleSpoStrField)))
  }

  private[triples] override def addObjectInfo(df: DataFrame): DataFrame = {
    val getObject = udf((spo: String) => {
      spo.substring(spo.lastIndexOf(tripleFieldsSeparator) + 1, spo.length).toLong
    })

    df.withColumn(tripleObjLongField, getObject(col(tripleSpoStrField)))
  }
  
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



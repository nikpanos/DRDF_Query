package gr.unipi.datacron.plans.physical.triples

import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.physical.traits.{filterByPOParams, filterByPOandKeepSpatioTemporalParams, pointSearchObjectParams}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import sun.reflect.generics.reflectiveObjects.NotImplementedException

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
  
  def filterByPO(params: filterByPOParams): DataFrame = {
    var searchStr = "^-?\\d+" + Consts.tripleFieldsSeparator

    searchStr += params.pred.getOrElse("-?\\d+") + Consts.tripleFieldsSeparator
    searchStr += params.obj.getOrElse("-?\\d+")

    params.df.filter(params.df(tripleSpoStrField) rlike searchStr)
  }

  def filterByPOandKeepSpatioTemporal(params: filterByPOandKeepSpatioTemporalParams): DataFrame = {
    throw new NotImplementedException()
  }
  
  def pointSearchObject(params: pointSearchObjectParams): Option[Long] = {
    val sp = params.sub + tripleFieldsSeparator + params.pred
    val searchStr = sp + tripleFieldsSeparator + "-?\\d+"
    try {
      val resStr = params.df.filter(params.df(tripleSpoStrField) rlike searchStr).first.getAs[String](tripleSpoStrField)
      Some(resStr.substring(resStr.indexOf(tripleFieldsSeparator) + 1).toLong)
    }
    catch {
      case ex: NoSuchElementException => None
    }
  }
}



package gr.unipi.datacron.operators.triples

import gr.unipi.datacron.operators.BaseOperator
import gr.unipi.datacron.operators.traits.TTriples
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.encoding._
import gr.unipi.datacron.common.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf

case class STriples() extends BaseOperator with TTriples {
  import DataStore.spark.implicits._
  
  private def addSubjectInfo(df: DataFrame): DataFrame = {
    val getSubject = udf((spo: String) => {
      spo.substring(0, spo.indexOf(tripleFieldsSeparator)).toLong
    })
    
    df.withColumn(tripleSubLongField, getSubject(col(tripleSpoStrField)))
  }
  
  private def addPredicateInfo(df: DataFrame): DataFrame = {
    val getPredicate = udf((spo: String) => {
      spo.substring(spo.indexOf(tripleFieldsSeparator) + 1, spo.lastIndexOf(tripleFieldsSeparator)).toLong
    })
    
    df.withColumn(triplePredLongField, getPredicate(col(tripleSpoStrField)))
  }
  
  private def addObjectInfo(df: DataFrame): DataFrame = {
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
    
    return df.filter(df(tripleSpoStrField) rlike searchStr)
  }
  
  def pointSearchObject(df: DataFrame, sub: Long, pred: Long): Option[Long] = {
    val sp = sub + tripleFieldsSeparator + pred
    val searchStr = sp + tripleFieldsSeparator + "-?\\d+"
    try {
      val resStr = df.filter(df(tripleSpoStrField) rlike searchStr).first.getAs[String](tripleSpoStrField)
      return Some(resStr.substring(resStr.indexOf(tripleFieldsSeparator) + 1).toLong)
    }
    catch {
      case ex: Exception => None  //TODO: fix the exception type here
    }
  }
  
  def filterBySubSpatioTemporalInfo(df: DataFrame, constraints: SpatioTemporalRange, encoder: SimpleEncoder): DataFrame = {
    
    val intervalIds = DataStore.temporalGrid.getIntervalIds(constraints)
    val spatialIds = DataStore.spatialGrid.getSpatialIds(constraints)
    
    val result = addSubjectInfo(df)
    
    val getPruneKey = udf((sub: Long) => {
      var key = -1
      if (sub >= 0) {
        val components = encoder.decodeComponentsFromKey(sub)
        
        if ((components._1 >= intervalIds._1) && (components._1 <= intervalIds._2)) {
          //not pruned by temporal
          val sp = spatialIds.get(components._2)
          if (sp.nonEmpty) {
            //not pruned by spatial
            key = 3  //initially set to need both refinements
            if ((components._1 > intervalIds._1) && (components._1 < intervalIds._2)) {
              //does not need temporal refinement
              key -= 1
            }
            if (!sp.get) {
              //does not need spatial refinement
              key -= 2
            }
          }
        }
      }
      key
    })
    
    result.withColumn(triplePruneSubKeyField, getPruneKey(col(tripleSubLongField))).filter(col(triplePruneSubKeyField) > -1)
  }
  
  def joinSubjectsWithNewObjects(df: DataFrame, dfTriples: DataFrame, predicates: Map[Long, String]): DataFrame = {
    val fr = if (df.columns.contains(tripleSubLongField)) df else addSubjectInfo(df)
    val subjects = fr.select(tripleSubLongField).as[Long].collect.toSet
    val bSubjects = DataStore.sc.broadcast(subjects)
    val bPredicates = DataStore.sc.broadcast(predicates)
    
    val tmp = dfTriples.flatMap(row => {
      val spo = row.getAs[String](tripleSpoStrField)
      val s = spo.substring(0, spo.indexOf(tripleFieldsSeparator)).toLong
      
      if (bSubjects.value.contains(s)) {
        val p = spo.substring(spo.indexOf(tripleFieldsSeparator) + 1, spo.lastIndexOf(tripleFieldsSeparator)).toLong
        
        if (bPredicates.value.contains(p)) {
          val o = spo.substring(spo.lastIndexOf(tripleFieldsSeparator) + 1, spo.length).toLong
          Some((s, p), o)
        }
        else {
          None
        }
      }
      else {
        None
      }
    }).collect.toMap
    val bTmp = DataStore.sc.broadcast(tmp)
    
    val getColumnValue = (pred: Long) => {udf((sub: Long) => {
      bTmp.value.get((sub, pred))
    })}
    
    var result = fr
    predicates.foreach(x => {
      result = result.withColumn(x._2, getColumnValue(x._1)(col(tripleSubLongField)))
    })
    result
  }
  
  def filterbySpatioTemporalRange(df: DataFrame, range: SpatioTemporalRange): DataFrame = {
    
    df.filter(x => {
      var tmpResult = ((x.getAs[Int](triplePruneSubKeyField) >> 0) & 1) != 1
      var sptResult = ((x.getAs[Int](triplePruneSubKeyField) >> 1) & 1) != 1
      
      if (!tmpResult) {
        //refine temporal
        val decodedObject = x.getAs[String](tripleTimeStartField + tripleTranslateSuffix).toLong
        if ((decodedObject >= range.low.time) && (decodedObject <= range.high.time)) {
          tmpResult = true
        }
      }
      
      if (!sptResult) {
        //refine spatial
        val decodedObject = x.getAs[String](tripleMBRField + tripleTranslateSuffix).substring(6)
        val lonlat = decodedObject.substring(0, decodedObject.length - 1).split(Consts.lonLatSeparator)
        val lon = lonlat(0).toDouble
        val lat = lonlat(1).toDouble
        if ((lon >= range.low.longitude) && (lon <= range.high.longitude) &&
            (lat >= range.low.latitude) && (lat <= range.high.latitude)) {
          sptResult = true
        }
        
      }
      
      tmpResult && sptResult
    })
  }
}



package gr.unipi.datacron.plans.physical.joinTriples

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.physical.BaseOperator
import gr.unipi.datacron.plans.physical.traits.TJoinTriples
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class MBJoinSTriples() extends BaseOperator with TJoinTriples {
  import DataStore.spark.implicits._

  override def joinSubjectsWithNewObjects(df: DataFrame, dfTriples: DataFrame, predicates: Map[Long, String]): DataFrame = {
    val subjects = df.select(tripleSubLongField).as[Long].collect.toSet
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

    val getColumnValue = (pred: Long) => {udf((sub: Long) => bTmp.value.get((sub, pred)))}

    var result = df
    predicates.foreach(x => {
      result = result.withColumn(x._2, getColumnValue(x._1)(col(tripleSubLongField)))
    })
    result
  }
}

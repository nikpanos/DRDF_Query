package gr.unipi.datacron.plans.physical.joinTriples

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.physical.BaseOperator
import gr.unipi.datacron.plans.physical.traits.TJoinTriples
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class MBJoinLLLTriples() extends BaseOperator with TJoinTriples {
  import DataStore.spark.implicits._

  override def joinSubjectsWithNewObjects(df: DataFrame, dfTriples: DataFrame, predicates: Map[Long, String]): DataFrame = {
    val subjects = df.select(tripleSubLongField).as[Long].collect.toSet
    val bSubjects = DataStore.sc.broadcast(subjects)
    val bPredicates = DataStore.sc.broadcast(predicates)

    val tmp = dfTriples.flatMap(row => {
      val s = row.getAs[Long](tripleSubLongField)
      if (bSubjects.value.contains(s)) {
        val p = row.getAs[Long](triplePredLongField)
        if (bPredicates.value.contains(p)) {
          val o = row.getAs[Long](tripleObjLongField)
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

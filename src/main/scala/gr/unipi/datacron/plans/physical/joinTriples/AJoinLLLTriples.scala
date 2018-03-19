package gr.unipi.datacron.plans.physical.joinTriples

import gr.unipi.datacron.common.Consts.{triplePredLongField, tripleSubLongField}
import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits.{TJoinTriples, joinDataframesParams, joinNewObjectsParams}
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class AJoinLLLTriples() extends BasePhysicalPlan with TJoinTriples {
  import DataStore.spark.implicits._

  private def joinNewObject(df: DataFrame, dfTriples: DataFrame, subjectColumn: String, predicate: (Long, String)): DataFrame = {
    val cols = df.columns.map(x => {col("df1." + x).alias(x)}) :+ col("df2.objLong").alias(predicate._2)

    val filtered = dfTriples.filter(col(triplePredLongField) === predicate._1).as("df2")

    //println("r1.size = " + df.count())
    //println("r2.size = " + filtered.count())

    df.as("df1").join(filtered, col("df1." + subjectColumn) === col("df2." + tripleSubLongField)).select(cols: _*)
  }

  override def joinNewObjects(params: joinNewObjectsParams): DataFrame = {
    var result: DataFrame = params.df
    for (x <- params.predicates) {
      result = joinNewObject(result, params.dfTriples, params.subjectColumn, x)
    }
    result
  }

  override def joinDataframes(params: joinDataframesParams): DataFrame =
    params.df1.as(params.df1Alias).join(params.df2.as(params.df2Alias), col(params.df1Alias + "." + params.df1JoinColumn) === col(params.df2Alias + "." + params.df2JoinColumn))

}

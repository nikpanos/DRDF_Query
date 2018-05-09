package gr.unipi.datacron.plans.physical.properties

import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.common.Consts._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class Properties() extends BasePhysicalPlan with TProperties {
  override def addTemporaryColumnForRefinement(params: addTemporaryColumnForRefinementParams): DataFrame = {
    val predicates = params.predicates

    val myUDf = udf((s:String) => {
      predicates.map(x => {
        val pos = s.indexOf(x.toString)
        if (pos >= 0) {
          val pos1 = s.indexOf(tripleFieldsSeparator, pos) + 1
          val pos2 = s.indexOf(tripleFieldsSeparator, pos1)
          if (pos2 >= 0) {
            s.substring(pos1, pos2)
          }
          else {
            s.substring(pos1)
          }
        }
        else {
          null
        }
      })
    })

    params.df.withColumn(tripleTemporaryRefinementField, myUDf(col(triplePropertiesStrField)))
  }

  override def addSpatialAndTemporalColumnsByTemporaryColumn(params: addSpatialAndTemporalColumnsByTemporaryColumnParams): DataFrame = {
    params.df.withColumn(tripleMBRField, col(tripleTemporaryRefinementField)(params.spatialColumn))
             .withColumn(tripleTimeStartField, col(tripleTemporaryRefinementField)(params.temporalColumn))
             .drop(tripleTemporaryRefinementField)
  }

  override def filterNullProperties(params: filterNullPropertiesParams): DataFrame = params.df.na.drop(params.columnNames)
}

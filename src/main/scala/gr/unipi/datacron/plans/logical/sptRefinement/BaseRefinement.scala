package gr.unipi.datacron.plans.logical.sptRefinement

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.{filterbySpatioTemporalRangeParams, prepareForFinalTranslationParams, decodeColumnsParams}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

private[sptRefinement] class BaseRefinement() {

  def decodeDatesAndRefineResult(dfTriples: DataFrame, constraints: SpatioTemporalRange): DataFrame = {
    val translatedExtendedTriples = PhysicalPlanner.decodeColumns(decodeColumnsParams(dfTriples, Array(tripleMBRField, tripleTimeStartField), Some("Add decoded spatial and temporal columns")))

    val result = PhysicalPlanner.filterbySpatioTemporalRange(filterbySpatioTemporalRangeParams(translatedExtendedTriples, constraints, tripleMBRField + tripleTranslateSuffix, tripleTimeStartField + tripleTranslateSuffix, Some("Filter by spatiotemporal columns")))

    //val result = translatedExtendedTriples

    //Translate the result before returning
    val outPrepared = PhysicalPlanner.prepareForFinalTranslation(prepareForFinalTranslationParams(result))
    val outTranslated = PhysicalPlanner.decodeColumns(decodeColumnsParams(outPrepared, Array(tripleSubLongField, triplePredLongField, tripleObjLongField), Some("Final decode of columns")))
    val outColumns = outTranslated.columns.filter(_.endsWith(tripleTranslateSuffix))
    outTranslated.select(outColumns.head, outColumns.tail: _*)
  }
}

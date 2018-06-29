package gr.unipi.datacron.plans.logical.staticPlans.sptRefinement

import gr.unipi.datacron.common.SpatioTemporalRange
import org.apache.spark.sql.DataFrame

private[sptRefinement] class BaseRefinement() {

  def decodeDatesAndRefineResult(dfTriples: DataFrame, constraints: SpatioTemporalRange): DataFrame = {
    //val translatedExtendedTriples = PhysicalPlanner.decodeColumns(decodeColumnsParams(dfTriples, Array(tripleMBRField, tripleTimeStartField), false, None))

    //val result = PhysicalPlanner.filterBySpatioTemporalRange(filterBySpatioTemporalRangeParams(translatedExtendedTriples, constraints, tripleMBRField + tripleTranslateSuffix, tripleTimeStartField + tripleTranslateSuffix, None))

    //val result = translatedExtendedTriples

    //Translate the result before returning
    /*val outPrepared = PhysicalPlanner.prepareForFinalTranslation(prepareForFinalTranslationParams(result))
    val outTranslated = PhysicalPlanner.decodeColumns(decodeColumnsParams(outPrepared, Array(tripleSubLongField, triplePredLongField, tripleObjLongField), false, Some("Final decode of columns")))
    val outColumns = outTranslated.columns.filter(_.endsWith(tripleTranslateSuffix))
    outTranslated.select(outColumns.head, outColumns.tail: _*)*/
    dfTriples
  }
}

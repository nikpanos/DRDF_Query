package gr.unipi.datacron.plans.logical.staticPlans.sptRefinement

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import org.apache.spark.sql.DataFrame

private[logical] case class PropertiesRefinement2() extends BaseRefinement {
  val encodedUriSpatialShortcut: Long = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(tripleMBRField, Some("Find encoded " + tripleMBRField))).get
  val encodedUriTemporalShortcut: Long = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(tripleTimeStartField, Some("Find encoded " + tripleTimeStartField))).get

  def refineResults(dfFilteredTriples: DataFrame, constraints: SpatioTemporalRange, qPredEncoded: Long, qObjEncoded: Long): DataFrame = {
    val translatedExtendedTriples = PhysicalPlanner.decodeColumns(decodeColumnsParams(dfFilteredTriples, Array(encodedUriSpatialShortcut.toString, encodedUriTemporalShortcut.toString), false, Some("Add decoded spatial and temporal columns")))

    val result = PhysicalPlanner.filterBySpatioTemporalRange(filterBySpatioTemporalRangeParams(translatedExtendedTriples, constraints, encodedUriSpatialShortcut.toString + tripleTranslateSuffix, encodedUriTemporalShortcut.toString + tripleTranslateSuffix, Some("Filter by spatiotemporal columns")))

    //val result = translatedExtendedTriples

    //Translate the result before returning
    /*val outPrepared = PhysicalPlanner.prepareForFinalTranslation(prepareForFinalTranslationParams(result))
    val outTranslated = PhysicalPlanner.decodeColumns(decodeColumnsParams(outPrepared, Array(tripleSubLongField, qPredEncoded.toString), false, Some("Final decode of columns")))
    val outColumns = outTranslated.columns.filter(_.endsWith(tripleTranslateSuffix))
    outTranslated.select(outColumns.head, outColumns.tail: _*)*/
    result
  }
}

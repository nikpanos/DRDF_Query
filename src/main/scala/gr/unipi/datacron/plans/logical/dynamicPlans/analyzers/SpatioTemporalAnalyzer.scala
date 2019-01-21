package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers
import java.text.SimpleDateFormat

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.{AppConfig, SpatioTemporalInfo, SpatioTemporalRange}
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.{BaseOperator, SortOperator}

import scala.util.Try

class SpatioTemporalAnalyzer extends PlanAnalyzer {
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  private var shouldApplyExactSpatioTemporalFilterLater = false

  private val constraints = Try(SpatioTemporalRange(
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatLower), AppConfig.getDouble(qfpLonLower), AppConfig.getOptionalDouble(qfpAltLower), dateFormat.parse(AppConfig.getString(qfpTimeLower)).getTime),
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatUpper), AppConfig.getDouble(qfpLonUpper), AppConfig.getOptionalDouble(qfpAltUpper), dateFormat.parse(AppConfig.getString(qfpTimeUpper)).getTime))).toOption

  /*private def refineBySpatioTemporalInfo(child: analyzedOperators.commonOperators.BaseOperator): analyzedOperators.commonOperators.BaseOperator = {
    if (shouldApplyExactSpatioTemporalFilterLater) {
      shouldApplyExactSpatioTemporalFilterLater = false
      analyzedOperators.spatiotemporalOperators.ExactBoxOperator(child, constraints.get, child.isPrefixed)
    }
    else {
      child
    }
  }

  private def getPushedDownSpatioTemporalOperator(df: DataFrame, child: analyzedOperators.commonOperators.BaseOperator): analyzedOperators.commonOperators.BaseOperator = {
    if (constraints.isDefined && df.hasSpatialAndTemporalShortcutCols) {
      val newOp = if (AppConfig.getOptionalBoolean(qfpEnableFilterByEncodedInfo).getOrElse(true)) {
        shouldApplyExactSpatioTemporalFilterLater = true
        analyzedOperators.spatiotemporalOperators.ApproximateBoxOperator(child, constraints.get, child.isPrefixed)
      } else { child }
      if (AppConfig.getBoolean(qfpEnableRefinementPushdown)) {
        refineBySpatioTemporalInfo(child)
      } else { newOp }
    }
    else { child }
  }*/
}

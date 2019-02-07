package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers
import java.text.SimpleDateFormat

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.{SpatioTemporalInfo, SpatioTemporalRange}
import gr.unipi.datacron.plans.logical.dynamicPlans.operands._
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.{ApproximateSpatioTemporalOperator, BaseOperator, SelectOperator}

class SpatioTemporalAnalyzer extends PlanAnalyzer {
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  private var shouldApplySpatioTemporalApproximation: Boolean = false

  private var semanticNodeColumnName: Option[String] = None

  private var constraints: Option[SpatioTemporalRange] = None


  private def findFunctionOperand(so: SelectOperator, functionName: String): Option[OperandFunction] = {

    def checkOperand(op: BaseOperand): Boolean = op match {
      case ofn: OperandFunction => ofn.getFunctionName == functionName
      case opr: OperandPair => checkOperand(opr.getLeftOperand) || checkOperand(opr.getRightOperand)
      case _ => false
    }

    so.getOperands.find(checkOperand).asInstanceOf[Option[OperandFunction]]
  }

  override protected def processSelectOperator(so: SelectOperator): BaseOperator = {
    val foO = findFunctionOperand(so, functionSpatioTemporalBox)
    if (foO.isDefined) {
      val fo = foO.get
      semanticNodeColumnName = Some(fo.getArguments.head.asInstanceOf[ColumnOperand].getColumn.getQueryString)
      val con = fo.getArguments.tail.map(_.asInstanceOf[ValueOperand].getValue)
      if (fo.getArguments.length == 7) {
        shouldApplySpatioTemporalApproximation = true
        constraints = Some(SpatioTemporalRange(
          SpatioTemporalInfo(con(0).toDouble, con(1).toDouble, None, dateFormat.parse(con(2)).getTime),
          SpatioTemporalInfo(con(3).toDouble, con(4).toDouble, None, dateFormat.parse(con(5)).getTime)))
      }
      else if (fo.getArguments.length == 9) {
        shouldApplySpatioTemporalApproximation = true
        constraints = Some(SpatioTemporalRange(
          SpatioTemporalInfo(con(0).toDouble, con(1).toDouble, Some(con(2).toDouble), dateFormat.parse(con(3)).getTime),
          SpatioTemporalInfo(con(4).toDouble, con(5).toDouble, Some(con(6).toDouble), dateFormat.parse(con(7)).getTime)))
      }
      else {
        throw new Exception("Wrong number of arguments in function: " + fo.getFunctionName)
      }
    }
    super.processSelectOperator(so)
  }

  private def containsQueryStringInSubject(so: SelectOperator, queryString: String): Boolean = {
    so.getSubject == queryString
  }

  override protected def processLowLevelSelectOperator(so: SelectOperator, ch: BaseOperator, isPropertyTableSource: Boolean): BaseOperator = {
    val child: BaseOperator = if (shouldApplySpatioTemporalApproximation && containsQueryStringInSubject(so, semanticNodeColumnName.get) && isPropertyTableSource) {
      ApproximateSpatioTemporalOperator(ch, constraints.get)
    }
    else {
      ch
    }
    super.processLowLevelSelectOperator(so, child, isPropertyTableSource)
  }


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

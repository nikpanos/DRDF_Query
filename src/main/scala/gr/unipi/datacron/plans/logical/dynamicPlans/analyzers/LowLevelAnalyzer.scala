package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers

import gr.unipi.datacron.common.Consts.{rdfType, tripleObjLongField, tripleSubLongField}
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.plans.logical.dynamicPlans.analyzers.PlanAnalyzer._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes.{OBJECT, PREDICATE}
import gr.unipi.datacron.plans.logical.dynamicPlans.operands.{ColumnOperand, OperandPair, ValueOperand}
import gr.unipi.datacron.plans.logical.dynamicPlans.operators._
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.encodeSingleValueParams
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

abstract class LowLevelAnalyzer extends BaseAnalyzer {

  val datasources: Array[DatasourceOperator] = DataStore.allData.map(DatasourceOperator)

  private def getEncodedStr(decodedColumnName: String): String = {
    val result = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(decodedColumnName))
    if (result.isEmpty){
      throw new Exception("Could not find encoded value for column name: " + decodedColumnName)
    }
    else {
      result.get.toString
    }
  }

  private def filterSelectOperators(so: SelectOperator, columnType: ColumnTypes): Array[OperandPair] = {
    so.getOperands.flatMap({case op: OperandPair =>
      op.getLeftOperand match {
        case column: ColumnOperand => Some(op)
      }
    })
  }

  private def getPredicateList(node: BaseOperator): Array[String] = {
    node match {
      case _: JoinSubjectOperator =>
        node.getBopChildren.foldLeft(Array[String]())((preds: Array[String], child: BaseOperator) => {
          preds ++ getPredicateList(child)
        })
      case so: SelectOperator =>
        filterSelectOperators(so, PREDICATE).map(operandPair => getEncodedStr(operandPair.getRightOperand.asInstanceOf[ValueOperand].getValue))
      case _ => throw new Exception("Only support JoinSubject and Select operators under JoinSubject")
    }
  }

  private def findSelectOperator(so: SelectOperator, columnType: ColumnTypes): Option[OperandPair] = {
    so.getOperands.find({ case operandPair: OperandPair =>
      operandPair.getLeftOperand match {
        case column: ColumnOperand =>
          column.getColumn.getColumnTypes == columnType
        case _ => throw new Exception("Should be ColumnOperand")
      }
    }).asInstanceOf[Option[OperandPair]]
  }

  private def findObjectOfRdfType(node: BaseOperator): Option[String] = {
    node match {
      case sNode: SelectOperator =>
        val objFilter = findSelectOperator(sNode, OBJECT)
        if (objFilter.isDefined) {
          Some(getEncodedStr(objFilter.get.getRightOperand.asInstanceOf[ValueOperand].getValue))
        }
        else None
      case jsNode: JoinSubjectOperator =>
        jsNode.getBopChildren.foreach(child => {
          val res = findObjectOfRdfType(child)
          if (res.isDefined) return res
        })
        None
    }
  }

  private def guessDataFrame(dsO: Option[DatasourceOperator], node: BaseOperator): Array[DatasourceOperator] = {
    if (dsO.isEmpty) {
      val rdfTypeEnc = getEncodedStr(rdfType)
      var predicates = getPredicateList(node)

      if ((predicates.length == 1) && (predicates(0) == rdfTypeEnc)) {
        node match {
          case so: SelectOperator =>
            val objFilter = findSelectOperator(so, OBJECT)
            if (objFilter.isDefined) {
              val encodedObjFilter = getEncodedStr(objFilter.get.getRightOperand.asInstanceOf[ValueOperand].getValue)
              return DataStore.findDataframeBasedOnRdfType(encodedObjFilter)
            }
        }
        DataStore.propertyData.filter(df => {
          df.getIncludingColumns(predicates).length > 0
        }) :+ DataStore.triplesData
      }
      else {
        if (predicates.contains(rdfTypeEnc)) {
          val objFilter = findObjectOfRdfType(node)
          if (objFilter.isDefined) {
            val dfA = DataStore.findDataframeBasedOnRdfType(objFilter.get)
            if (!dfA.contains(DataStore.nodeData)) {
              return dfA
            }
          }
        }
        predicates = predicates.filter(!_.equals(rdfTypeEnc))

        val result = DataStore.propertyData.filter(df => {
          df.getIncludingColumns(predicates).length > 0
        })

        if (result.length == 0) {
          Array(DataStore.triplesData)
        }
        else if (result.length == 1) {
          val df = result(0)
          val excl = df.getExcludingColumns(predicates)
          if (excl.length > 0) {
            Array(df, DataStore.triplesData)
          }
          else {
            Array(df)
          }
        }
        else {
          throw new Exception("Does not support more than one dataframes")
        }
      }
    }
    else {
      Array(dfO.get)
    }
  }

  private def findFirstDatasourceOperator(bo: BaseOperator): DatasourceOperator = {
    bo match {
      case ds: DatasourceOperator => ds
      case _=> findFirstDatasourceOperator(bo)
    }
  }

  private def convertSelectToUnionOperator(so: SelectOperator, dfs: Array[DataFrame]): UnionOperator = {
    val selOps = dfs.map(df => {
      val bo = processNode(so, Some(df))
      val ds = findFirstDatasourceOperator(bo)
      if (ds.isPropertyTableSource) {
        val colName = getEncodedStr(findSelectOperator(so, PREDICATE).get.getRightOperand.asInstanceOf[ValueOperand].getValue)
        analyzedOperators.columnOperators.ProjectOperator(bo, Array(tripleSubLongField, colName), false)
      }
      else {
        bo
      }
    })
    analyzedOperators.dataOperators.UnionOperator(selOps, false)
  }

  private def processLowLevelSelectOperator(so: SelectOperator, dfO: Option[DataFrame]): BaseOperator = {
    val dfs = guessDataFrame(dfO, so)
    if (dfs.length > 1) {
      convertSelectToUnionOperator(so, dfs)
    }
    else {
      val dso = DatasourceOperator(dfs(0))

      //val spto = getPushedDownSpatioTemporalOperator(dfs(0), dso)

      val selOp = createSelectOperator(so, dso)
      if (!dso.isPropertyTableSource) {
        val po = analyzedOperators.columnOperators.ProjectOperator(selOp, Array(tripleSubLongField, tripleObjLongField), false)
        val newColName = getEncodedStr(findSelectOperator(so, PREDICATE).get.getRightOperand.asInstanceOf[ValueOperand].getValue)
        analyzedOperators.columnOperators.RenameOperator(po, Array((tripleObjLongField, newColName)), false)
      }
      else {
        selOp
      }
    }
  }
}

package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers

import java.util

import gr.unipi.datacron.common.Consts.{rdfType, tripleObjLongField, triplePredLongField, tripleSubLongField}
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.{Column, ColumnTypes, ConditionType}
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes.{OBJECT, PREDICATE, SUBJECT}
import gr.unipi.datacron.plans.logical.dynamicPlans.operands._
import gr.unipi.datacron.plans.logical.dynamicPlans.operators._
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.encodeSingleValueParams

import scala.collection.mutable
import scala.util.Try

abstract class LowLevelAnalyzer extends BaseAnalyzer {

  private val rdfTypeEnc = getEncodedStr(rdfType)

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

  private def guessDatasource(dsO: Option[DatasourceOperator], node: BaseOperator): Array[DatasourceOperator] = {
    if (dsO.isEmpty) {
      val predicates = getPredicateList(node)
      if ((predicates.length == 1) && (predicates(0) == rdfTypeEnc)) {
        node match {
          case so: SelectOperator =>
            val objFilter = findSelectOperator(so, OBJECT)
            if (objFilter.isDefined) {
              val encodedObjFilter = getEncodedStr(objFilter.get.getRightOperand.asInstanceOf[ValueOperand].getValue)
              return Datasources.findDatasourceBasedOnRdfType(encodedObjFilter)
            }
        }
        Datasources.getDatasourcesByPredicates(predicates)
      }
      else {
        if (predicates.contains(rdfTypeEnc)) {
          val objFilter = findObjectOfRdfType(node)
          if (objFilter.isDefined) {
            /*val dfA = Datasources.findDatasourceBasedOnRdfType(objFilter.get)
            if (!dfA.contains(DataStore.nodeData)) {
              return dfA
            }*/
            Datasources.findDatasourceBasedOnRdfType(objFilter.get)
          }
        }
        Datasources.getAllDatasourcesByIncludingAndExcludingPredicates(predicates.filter(!_.equals(rdfTypeEnc)))
      }
    }
    else {
      Array(dsO.get)
    }
  }

  private def findFirstDatasourceOperator(bo: BaseOperator): DatasourceOperator = {
    bo match {
      case ds: DatasourceOperator => ds
      case _=> findFirstDatasourceOperator(bo)
    }
  }

  private def convertSelectToUnionOperator(so: SelectOperator, dfs: Array[DatasourceOperator]): UnionOperator = {
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

  private def getOperandPairOfColumn(so: SelectOperator, cType: ColumnTypes): Option[OperandPair] = {
    Try(so.getOperands.find({
      case op: OperandPair =>
        op.getLeftOperand match {
          case co: ColumnOperand =>
            co.getColumn.getColumnTypes == cType
        }
    }).get.asInstanceOf[OperandPair]).toOption
  }

  protected def processLowLevelSelectOperator(so: SelectOperator, dsO: Option[DatasourceOperator]): BaseOperator = {
    val dss = guessDatasource(dsO, so)
    if (dss.length > 1) {
      convertSelectToUnionOperator(so, dss)
    }
    else {
      val ds = dss(0)
      val subOp = getOperandPairOfColumn(so, SUBJECT)
      val predOp = getOperandPairOfColumn(so, PREDICATE)
      val objOp = getOperandPairOfColumn(so, OBJECT)

      val operands = mutable.ListBuffer[BaseOperand]()
      if (subOp.isDefined) {
        val encodedValue = getEncodedStr(subOp.get.getRightOperand.asInstanceOf[ValueOperand].getValue)
        val operand = OperandPair.newOperandPair(ColumnNameOperand(tripleSubLongField), ValueOperand.newValueOperand(encodedValue), ConditionType.EQ)
        operands.append(operand)
      }

      val encodedFilterPred = getEncodedStr(predOp.get.getRightOperand.asInstanceOf[ValueOperand].getValue)

      if (ds.isPropertyTableSource) {
        if (objOp.isDefined) {
          val encodedFilterObj = getEncodedStr(objOp.get.getRightOperand.asInstanceOf[ValueOperand].getValue)
          val operand = OperandPair.newOperandPair(ColumnNameOperand(encodedFilterPred), ValueOperand.newValueOperand(encodedFilterObj), ConditionType.EQ)
          operands.append(operand)
        }
        else {
          operands.append(NotNullOperand(encodedFilterPred))
        }
        so.setOperands(operands.toArray)
        so
      }
      else {
        val operand = OperandPair.newOperandPair(ColumnNameOperand(triplePredLongField), ValueOperand.newValueOperand(encodedFilterPred), ConditionType.EQ)
        operands.append(operand)
        //df = PhysicalPlanner.filterByColumn(filterByColumnParams(df, triplePredLongField, encodedFilterPred, Option(filter)))
        if (objOp.isDefined) {
          val encodedFilterObj = getEncodedStr(objOp.get.getRightOperand.asInstanceOf[ValueOperand].getValue)
          val operand = OperandPair.newOperandPair(ColumnNameOperand(tripleObjLongField), ValueOperand.newValueOperand(encodedFilterObj), ConditionType.EQ)
        }
        ProjectOperator.newProjectOperator(so, Array(tripleSubLongField, tripleObjLongField))
        val map = new util.HashMap[Column, Column]()
        map.put(Column.newColumn(tripleObjLongField, "", ColumnTypes.OBJECT), Column.newColumn(encodedFilterPred, "", ColumnTypes.OBJECT))
        RenameOperator.newRenameOperator(so, map)
        //df = PhysicalPlanner.dropColumns(dropColumnsParams(df, Array(triplePredLongField)))
        //df = PhysicalPlanner.renameColumns(renameColumnsParams(df, Map((tripleObjLongField, encodedFilterPred))))
      }
    }
  }
}

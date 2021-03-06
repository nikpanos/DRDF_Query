package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers

import java.util

import gr.unipi.datacron.common.Consts.{rdfType, tripleObjLongField, triplePredLongField, tripleSubLongField}
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.{Column, ColumnTypes, ConditionType, SparqlColumn}
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes.{OBJECT, PREDICATE, SUBJECT}
import gr.unipi.datacron.plans.logical.dynamicPlans.operands._
import gr.unipi.datacron.plans.logical.dynamicPlans.operators._

import scala.collection.mutable
import scala.util.Try

abstract class LowLevelAnalyzer extends BaseAnalyzer {

  private val rdfTypeEnc = getEncodedStr(rdfType)

  private def filterSelectOperators(so: SelectOperator, columnType: ColumnTypes): Array[PairOperand] = {
    so.getOperands.flatMap({case op: PairOperand =>
      op.getLeftOperand match {
        case _: ColumnOperand => Some(op)
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

  private def findSelectOperator(so: SelectOperator, columnType: ColumnTypes): Option[PairOperand] = {
    so.getOperands.find({ case operandPair: PairOperand =>
      operandPair.getLeftOperand match {
        case column: ColumnOperand =>
          column.getColumn.getColumnTypes == columnType
        case _ => throw new Exception("Should be ColumnOperand")
      }
    }).asInstanceOf[Option[PairOperand]]
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
            return Datasources.findDatasourceBasedOnRdfType(objFilter.get)
          }
        }
        Datasources.getAllDatasourcesByIncludingAndExcludingPredicates(predicates.filter(!_.equals(rdfTypeEnc)))
      }
    }
    else {
      Array(dsO.get)
    }
  }

  private def convertSelectToUnionOperator(so: SelectOperator, dss: Array[DatasourceOperator]): BaseOperator = {
    val selOps = dss.map(ds => {
      val bo = processLowLevelSelectOperator(so, ds, ds.isPropertyTableSource)
      //val ds = findFirstDatasourceOperator(bo)
      if (ds.isPropertyTableSource) {
        val colName = getEncodedStr(findSelectOperator(so, PREDICATE).get.getRightOperand.asInstanceOf[ValueOperand].getValue)
        ProjectOperator.newProjectOperator(bo, Array(tripleSubLongField, colName))
      }
      else {
        bo
      }
    })
    selOps.tail.foldLeft(selOps.head)((left, right) => {
      UnionOperator.newUnionOperator(left, right)
    })
  }

  private def getOperandPairOfColumn(so: SelectOperator, cType: ColumnTypes): Option[PairOperand] = {
    Try(so.getOperands.find({
      case op: PairOperand =>
        op.getLeftOperand match {
          case co: ColumnOperand =>
            co.getColumn.getColumnTypes == cType
        }
    }).get.asInstanceOf[PairOperand]).toOption
  }

  protected def processLowLevelSelectOperator(so: SelectOperator, ch: BaseOperator, isPropertyTableSource: Boolean): BaseOperator = {
    val subOp = getOperandPairOfColumn(so, SUBJECT)
    val predOp = getOperandPairOfColumn(so, PREDICATE)
    val objOp = getOperandPairOfColumn(so, OBJECT)

    val operands = mutable.ListBuffer[BaseOperand]()
    if (subOp.isDefined) {
      val encodedValue = getEncodedStr(subOp.get.getRightOperand.asInstanceOf[ValueOperand].getValue)
      val operandSub = PairOperand.newOperandPair(ColumnNameOperand(tripleSubLongField), ValueOperand.newValueOperand(encodedValue), ConditionType.EQ)
      operands.append(operandSub)
    }

    val encodedFilterPred = getEncodedStr(predOp.get.getRightOperand.asInstanceOf[ValueOperand].getValue)

    if (isPropertyTableSource) {
      if (objOp.isDefined) {
        val encodedFilterObj = getEncodedStr(objOp.get.getRightOperand.asInstanceOf[ValueOperand].getValue)
        val operand = PairOperand.newOperandPair(ColumnNameOperand(encodedFilterPred), ValueOperand.newValueOperand(encodedFilterObj), ConditionType.EQ)
        operands.append(operand)
      }
      else {
        operands.append(NotNullOperand(encodedFilterPred))
      }
      SelectOperator.newSelectOperator(ch, so.getArrayColumns, operands.toArray, so.getOutputSize)
    }
    else {
      val operandPred = PairOperand.newOperandPair(ColumnNameOperand(triplePredLongField), ValueOperand.newValueOperand(encodedFilterPred), ConditionType.EQ)
      operands.append(operandPred)
      //df = PhysicalPlanner.filterByColumn(filterByColumnParams(df, triplePredLongField, encodedFilterPred, Option(filter)))
      if (objOp.isDefined) {
        val encodedFilterObj = getEncodedStr(objOp.get.getRightOperand.asInstanceOf[ValueOperand].getValue)
        val operandObj = PairOperand.newOperandPair(ColumnNameOperand(tripleObjLongField), ValueOperand.newValueOperand(encodedFilterObj), ConditionType.EQ)
        operands.append(operandObj)
      }
      val newSo = SelectOperator.newSelectOperator(ch, so.getArrayColumns, operands.toArray, so.getOutputSize)
      val po = ProjectOperator.newProjectOperator(newSo, Array(tripleSubLongField, tripleObjLongField))
      val map = new util.HashMap[SparqlColumn, SparqlColumn]()
      map.put(SparqlColumn.newSparqlColumn(tripleObjLongField, "", ColumnTypes.OBJECT), SparqlColumn.newSparqlColumn(encodedFilterPred, "", ColumnTypes.OBJECT))
      RenameOperator.newRenameOperator(po, map)
      //df = PhysicalPlanner.dropColumns(dropColumnsParams(df, Array(triplePredLongField)))
      //df = PhysicalPlanner.renameColumns(renameColumnsParams(df, Map((tripleObjLongField, encodedFilterPred))))
    }
  }

  override protected def processLeafSelectOperator(so: SelectOperator): BaseOperator = {
    val dss = guessDatasource(None, so)
    if (dss.length > 1) {
      convertSelectToUnionOperator(so, dss)
    }
    else {
      val ds = dss(0)
      processLowLevelSelectOperator(so, ds, ds.isPropertyTableSource)
    }
  }

  override protected def processJoinSubjectOperator(js: JoinSubjectOperator): BaseOperator = {

    def getSubjectColForOperator(op: BaseOperator): SparqlColumn = op.getArrayColumns.find(_.getColumnTypes == SUBJECT).get

    def getPrefixedSelectOperator(op: SelectOperator, ds: DatasourceOperator): BaseOperator = {
      val subCol = getSubjectColForOperator(op)
      prefixNode(op, subCol, processLowLevelSelectOperator(op, ds, ds.isPropertyTableSource))
    }

    def processPropertyTable(selectOps: Array[SelectOperator], ds: DatasourceOperator): BaseOperator = {
      val firstSo = processLowLevelSelectOperator(selectOps.head, ds, ds.isPropertyTableSource)
      selectOps.tail.foldLeft(firstSo)((child, so) => {
        processLowLevelSelectOperator(so, child, ds.isPropertyTableSource)
      })
    }

    def processTriplesTable(selectOps: Array[SelectOperator], ds: DatasourceOperator): BaseOperator = {
      val firstSo = getPrefixedSelectOperator(selectOps.head, ds)
      val leftColName = getPrefixedColumnNameForOperation(selectOps.head, getSubjectColForOperator(selectOps.head), firstSo)

      selectOps.tail.foldLeft(firstSo)((left, so) => {
        val right = getPrefixedSelectOperator(so, ds)
        val rightColName = getPrefixedColumnNameForOperation(so, getSubjectColForOperator(so), right)
        val op = PairOperand.newOperandPair(ColumnNameOperand(leftColName), ColumnNameOperand(rightColName), ConditionType.EQ)
        JoinOperator.newJoinOperator(left, right, op)
      })
    }

    val dss = guessDatasource(None, js)
    if (dss.length > 1) {
      if (dss.length > 2) {
        throw new Exception("More than 2 datasources are not expected here")
      }
      val propertyDs = dss.find(_.isPropertyTableSource).get
      val triplesDs = dss.find(!_.isPropertyTableSource).get
      val (propertySo, triplesSo) = js.getBopChildren.map(_.asInstanceOf[SelectOperator]).partition(so => {
        val encPred = getEncodedStr(so.getPredicate)
        propertyDs.hasColumn(encPred)
      })
      val propertyTree = prefixNode(propertySo(0), getSubjectColForOperator(propertySo(0)), processPropertyTable(propertySo, propertyDs))
      val triplesTree = prefixNode(triplesSo(0), getSubjectColForOperator(triplesSo(0)), processTriplesTable(triplesSo, triplesDs))

      val propertyColName = getPrefixedColumnNameForOperation(propertySo(0), getSubjectColForOperator(propertySo(0)), propertyTree)
      val triplesColName = getPrefixedColumnNameForOperation(triplesSo(0), getSubjectColForOperator(triplesSo(0)), triplesTree)

      val op = PairOperand.newOperandPair(ColumnNameOperand(propertyColName), ColumnNameOperand(triplesColName), ConditionType.EQ)
      JoinOperator.newJoinOperator(propertyTree, triplesTree, op)
    }
    else {
      if (dss(0).isPropertyTableSource) {
        processPropertyTable(js.getBopChildren.map(_.asInstanceOf[SelectOperator]), dss(0))
      }
      else {
        processTriplesTable(js.getBopChildren.map(_.asInstanceOf[SelectOperator]), dss(0))
      }
    }
  }
}

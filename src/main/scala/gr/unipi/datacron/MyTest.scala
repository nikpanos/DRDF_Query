package gr.unipi.datacron

import gr.unipi.datacron.plans.logical.dynamicPlans.operators.{BaseOperator, FilterOf, JoinOperator, JoinOrOperator}
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.MyOpVisitorBase

import collection.JavaConverters._

object MyTest {
  def main(args : Array[String]) {
    val bop = MyOpVisitorBase.newMyOpVisitorBase(
      "Prefix : <http://www.datacron-project.eu/datAcron#>\n" +
      "SELECT *\n" +
        "WHERE\n" + "{\n" +
        "    ?n :ofMovingObject ?ves ;\n" +
        "    :hasGeometry ?g ;\n" +
        "    :hasTemporalFeature ?t ;\n" +
        "    :hasHeading ?heading ;\n" +
        "    :hasSpeed ?speed .\n" +
        "}\n").getBop

    val f = bop(0)

    val p = "http://www.datacron-project.eu/datAcron#"

    println(f + "\n\n\n\n\n")

    val incl = Array(p + "ofMovingObject", p + "hasGeometry", p + "hasTemporalFeature")
    val excl = Array(p + "hasHeading", p + "hasSpeed")

    convertJoinOr(bop(0).asInstanceOf[JoinOrOperator], incl, excl)
  }

  private def getNewJoinOrOperator(node: JoinOrOperator, preds: Array[String]): JoinOrOperator = {
    val filters = node.getBopChildren.asScala.toArray.filter(op => {
      preds.contains(op.asInstanceOf[FilterOf].getPredicate)
    })
    JoinOrOperator.newJoinOrOperator(filters: _*)
  }

  private def convertJoinOr(joinOrOperator: JoinOrOperator, incl: Array[String], excl: Array[String]): JoinOperator = {
    val joinOr1 = getNewJoinOrOperator(joinOrOperator, incl)
    println(joinOr1 + "\n\n\n\n\n")

    val joinOr2 = getNewJoinOrOperator(joinOrOperator, excl)
    println(joinOr2 + "\n\n\n\n\n")

    JoinOperator.newJoinOperator(joinOr1, joinOr2)
  }
}

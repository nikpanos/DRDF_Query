package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators

abstract class Base2ChildOperator(leftChild: BaseOperator, rightChild: BaseOperator) extends BaseOperator(Array(leftChild, rightChild)) {

}

package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators

abstract class Base2ChildOperator(leftChild: BaseOperator, rightChild: BaseOperator, prefixed: Boolean) extends BaseOperator(Array(leftChild, rightChild), prefixed) {

}

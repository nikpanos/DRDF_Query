/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

/**
 * @author nicholaskoutroumanis
 */
public abstract class BaseOpW2Child extends BaseOperator {

    private static final int left_child = 0;
    private static final int right_child = 1;

    public BaseOperator getLeftChild() {
        return getBopChildren()[left_child];
    }

    public BaseOperator getRightChild() {
        return getBopChildren()[right_child];
    }

    protected BaseOpW2Child(BaseOperator bopLeft, BaseOperator bopRight) {
        super(bopLeft, bopRight);
    }
}

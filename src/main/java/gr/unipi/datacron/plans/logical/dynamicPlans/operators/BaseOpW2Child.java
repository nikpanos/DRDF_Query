/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

/**
 * @author nicholaskoutroumanis
 */
public class BaseOpW2Child extends BaseOperator {


    @Override
    protected int getMaxNumberOfChildren() {
        return 2;
    }

    public BaseOperator getLeftChild() {
        return getBopChildren()[0];
    }

    public BaseOperator getRightChild() {
        return getBopChildren()[1];
    }

    protected BaseOpW2Child(BaseOperator bopLeft, BaseOperator bopRight) {
        super(bopLeft, bopRight);
    }
}

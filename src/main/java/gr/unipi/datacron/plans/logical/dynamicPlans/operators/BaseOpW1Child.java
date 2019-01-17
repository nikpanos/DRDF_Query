/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

/**
 * @author nicholaskoutroumanis
 */
public abstract class BaseOpW1Child extends BaseOperator {



    public BaseOperator getChild() {
        return getBopChildren()[0];
    }

    protected BaseOpW1Child(BaseOperator bop) {
        super(bop);
    }

    protected BaseOpW1Child(long outputSize, BaseOperator bop) {
        super(outputSize, bop);
    }

    public void setNewChild(BaseOperator newChild) {
        super.setNewChild(newChild, 0);
    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

/**
 * @author nicholaskoutroumanis
 */
public abstract class BaseOpWnChild extends BaseOperator {

    protected BaseOpWnChild(BaseOperator... bop) {
        super(bop);
    }

    public void setNewChildren(BaseOperator... newChildren) {
        for (int i = 0; i < newChildren.length; i++) {
            super.setNewChild(newChildren[i], i);
        }
    }
}

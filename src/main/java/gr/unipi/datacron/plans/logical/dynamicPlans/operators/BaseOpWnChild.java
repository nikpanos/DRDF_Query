/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

/**
 *
 * @author nicholaskoutroumanis
 */
public class BaseOpWnChild extends BaseOperator  {
    @Override
    protected int getMaxNumberOfChildren() {
        return Integer.MAX_VALUE;
    }      
}

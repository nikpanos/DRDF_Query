/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.test;

import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.MyOpVisitorBase;

/**
 *
 * @author nicholaskoutroumanis
 */
public class JoinOrOperatorTester {

    public static void main(String args[]) {

//        JoinOrOperator bop = JoinOrOperator.newJoinOrOperator(FilterOf.newFilterOf("?x", "tr", "45"),FilterOf.newFilterOf("?x", "hg", "mm"),
//                FilterOf.newFilterOf("?x", "fd", "?y"),FilterOf.newFilterOf("?x", "fv", "nn"));
        BaseOperator[] bop = MyOpVisitorBase.newMyOpVisitorBase("SELECT ?x"
                + "WHERE"
                + "{"
                + " ?x <tr> 45 ."
                + " ?x <hg> 'mm' ."
                + " ?x <fd> ?y ."
                + " ?x <fv> 'nn' ."
                + "}").getBop();

        System.out.println("NumberOfTrees: " + bop.length);
        for(BaseOperator b:bop){
            System.out.println("--------------------------");
            System.out.println(bop[0].toString());
        }
    }
}

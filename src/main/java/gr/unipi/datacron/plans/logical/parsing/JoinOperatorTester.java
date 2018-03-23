/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.parsing;

import gr.unipi.datacron.plans.logical.parsing.operators.BaseOperator;

/**
 *
 * @author nicholaskoutroumanis
 */
public class JoinOperatorTester {

    public static void main(String args[]) {

        BaseOperator[] bop = MyOpVisitorBase.newMyOpVisitorBase("SELECT ?x"
                + "WHERE"
                + "{"
                + " ?k <tr> 45 ."
                + " ?x <hg> ?k ."
                + "}").getBop();

        System.out.println("NumberOfTrees: " + bop.length);
        for(BaseOperator b:bop){
            System.out.println("--------------------------");
            System.out.println(bop[0].toString());
        }
    }
}

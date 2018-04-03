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
public class ShortcutsTest {

    public static void main(String args[]) {

        BaseOperator[] bop = MyOpVisitorBase.newMyOpVisitorBase("SELECT ?x"
                + "WHERE"
                + "{"
                + " ?a <:hasGeometry> ?e ."
                + " ?ds <:hasWKT> ?d ."
                + " ?e <:hasWKT> ?b ."
                + " ?b <:hasGeometry> ?r ."
                + " ?r <:hasWKT> ?c ."
                + " ?c <:hasGeometry> ?ds ."
                + "}").getBop();

        System.out.println("NumberOfTrees: " + bop.length);
        for(BaseOperator b:bop){
            System.out.println("--------------------------");
            System.out.println(bop[0].toString());
        }
    }
}

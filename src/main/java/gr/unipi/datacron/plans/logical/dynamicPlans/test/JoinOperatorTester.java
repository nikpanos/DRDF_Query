/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.test;

import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.LogicalPlanner;

/**
 * @author nicholaskoutroumanis
 */
public class JoinOperatorTester {

    public static void main(String args[]) {

        BaseOperator bop = LogicalPlanner.setSparqlQuery("SELECT ?x"
                + "WHERE"
                + "{"
                + " ?k <tr> 45 ."
                + " ?x <hg> ?w ."
                + " ?q <g> ?z ."
                + "}").build().getBop();

        System.out.println("--------------------------");
        System.out.println(bop.toString());

    }
}

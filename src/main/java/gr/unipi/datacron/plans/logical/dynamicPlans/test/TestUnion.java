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
public class TestUnion {

    public static void main(String[] args){


        BaseOperator bop = LogicalPlanner.setSparqlQuery(
                "PREFIX dc10:  <http://purl.org/dc/elements/1.0/>\n" +
                        "PREFIX dc11:  <http://purl.org/dc/elements/1.1/>\n" +
                        "\n" +
                        "SELECT ?title\n" +
                        "WHERE  { { ?book dc10:title  ?title } " +
                        "UNION" +
                        " { ?textbook dc11:title  ?title } }").build().getRoot();

        System.out.println("--------------------------");
        System.out.println(bop.toString());

    }
}

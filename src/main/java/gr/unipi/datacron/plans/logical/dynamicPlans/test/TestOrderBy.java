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
public class TestOrderBy {

    public static void main(String[] args){


        BaseOperator bop = LogicalPlanner.setSparqlQuery(
                "Prefix : <http://www.datacron-project.eu/datAcron#>" +
                        "Prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
                        "SELECT DISTINCT ?x ?y " +
                        "WHERE" +
                        "{" +
                        " ?x rdf:type ?y ." +
                        "FILTER (?x < 123) ." +
                        "FILTER (30 < ?x) ." +
                        "}" +
                        "ORDER BY ASC (?x) DESC(?y)").build().getRoot();

        System.out.println("--------------------------");
        System.out.println(bop.toString());

    }
}

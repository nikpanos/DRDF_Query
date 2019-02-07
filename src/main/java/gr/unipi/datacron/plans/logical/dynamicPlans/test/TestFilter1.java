package gr.unipi.datacron.plans.logical.dynamicPlans.test;

import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.LogicalPlanner;

public class TestFilter1 {

    public static void main(String[] args){
        BaseOperator bop = LogicalPlanner.setSparqlQuery(
                "Prefix : <http://www.datacron-project.eu/datAcron#>" +
                        "Prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
                        "SELECT ?x " +
                        "WHERE" +
                        "{" +
                        " ?x rdf:type ?y ." +
                        "FILTER (?x) ." +
                        "}").build().getRoot();

        System.out.println("--------------------------");
        System.out.println(bop.toString());

        BaseOperator bop1 = LogicalPlanner.setSparqlQuery(
                "Prefix : <http://www.datacron-project.eu/datAcron#>" +
                        "Prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
                        "SELECT ?x " +
                        "WHERE" +
                        "{" +
                        " ?x rdf:type ?y ." +
                        "FILTER (true) ." +
                        "}").build().getRoot();

        System.out.println("--------------------------");
        System.out.println(bop1.toString());
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.test;

import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.ProjectOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.SelectOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.LogicalPlanner;

/**
 *
 * @author nicholaskoutroumanis
 */
public class TestDistinct {

    public static void main(String args[]) {


        BaseOperator[] bop = LogicalPlanner.setSparqlQuery(
"Prefix : <http://www.datacron-project.eu/datAcron#>"+
"Prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
"SELECT DISTINCT ?x ?y " +
"WHERE" +
"{" +
" ?x rdf:type ?y" +
"}").build().getBop();
        bop[0].getBopChildren().get(0);

        ((ProjectOperator) bop[0]).getVariables().forEach(e->System.out.println(e));

        System.out.println("SUBJECT: "+((SelectOperator) bop[0].getBopChildren().get(0)).getSubject());
        System.out.println("PREDICATE: "+((SelectOperator) bop[0].getBopChildren().get(0)).getSubject());
        System.out.println("OBJECT: "+((SelectOperator) bop[0].getBopChildren().get(0)).getSubject());

       
        System.out.println("NumberOfTrees: " + bop.length);
        for(BaseOperator b:bop){
            System.out.println("--------------------------");
            System.out.println(bop[0].toString());
        }
    }
}

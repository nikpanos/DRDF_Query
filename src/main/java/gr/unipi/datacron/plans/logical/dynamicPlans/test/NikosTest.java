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
public class NikosTest {


    public static void main(String args[]) {
        //AppConfig.init("C:\\Users\\nikp\\Desktop\\params.hocon");

        //Long encodedValue = getRedisEncodedValue("a");
        //System.out.println(encodedValue);

        //String decodedValue = getRedisDecodedValue(-5L);
        //System.out.println(decodedValue);

        BaseOperator bop = LogicalPlanner.setSparqlQuery("SELECT ?x"
                + "WHERE"
                + "{"
                + " ?w <:sdfasdf> 25 ."
                + " ?zx <:afdasdf> 50 ."
                + " ?x <:dfghdfgh> 53 ."
                + " ?q <:hasGdfgheometry> 55 ."
                + " ?w <:wert> ?y ."
                + " ?k <:bedfg> 12 ."
                + " ?s <:vwdfgvdw> ?l ."
                + " ?x <:gwergwerg> 45 ."
                + " ?c <:irtui> ?k ."
                + " ?x <:wrtwr> 48 ."
                + " ?z <:qwerf> ?zx ."
                + "}").build().getBop();

        System.out.println("--------------------------");
        System.out.println(bop.toString());

    }
}

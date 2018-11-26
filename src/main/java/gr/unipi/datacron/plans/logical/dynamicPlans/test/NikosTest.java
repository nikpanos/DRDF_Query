/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.test;

import gr.unipi.datacron.common.AppConfig;
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.LogicalPlanner;

import java.util.Arrays;
import java.util.List;

/**
 *
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


//        List<Integer> l = Arrays.asList(50,40,70,80,30,10,20,90,60);
//        l.sort((bo1,bo2)->Long.compare(bo1.intValue(), bo2.intValue()));
//        System.out.println(l.toString());

    }
}

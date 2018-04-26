/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.test;

import gr.unipi.datacron.common.AppConfig;
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.FilterOf;
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.MyOpVisitorBase;
import gr.unipi.datacron.store.DataStore;
import scala.Option;

/**
 *
 * @author nicholaskoutroumanis
 */
public class PanosTest {

    private static Long getRedisEncodedValue(String key) {
        Option<Object> optionValue = DataStore.dictionaryRedis().getEncodedValue(key);
        Long value = null;
        if (optionValue.isDefined()) {
            value = (Long) optionValue.get();
        }
        return value;
    }

    private static String getRedisDecodedValue(Long key) {
        Option<String> optionValue = DataStore.dictionaryRedis().getDecodedValue(key);
        String value = null;
        if (optionValue.isDefined()) {
            value = optionValue.get();
        }
        return value;
    }

    public static void main(String args[]) {
        //AppConfig.init("C:\\Users\\nikp\\Desktop\\params.hocon");

        //Long encodedValue = getRedisEncodedValue("a");
        //System.out.println(encodedValue);

        //String decodedValue = getRedisDecodedValue(-5L);
        //System.out.println(decodedValue);

        BaseOperator[] bop = MyOpVisitorBase.newMyOpVisitorBase(
                "Prefix : <http://www.datacron-project.eu/datAcron#>\n" +
                          "Prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                          "SELECT *\n" +
                          "WHERE\n" +
                          "{\n" +
                          "    :nodeA a :Node\n" +
                          "}").getBop();
        
        System.out.println("NumberOfTrees: " + bop.length);
        for(BaseOperator b:bop){
            System.out.println("--------------------------");
            System.out.println(bop[0].toString());
            System.out.println(((FilterOf) bop[0]).getSubject());
            System.out.println(((FilterOf) bop[0]).getPredicate());
            System.out.println(((FilterOf) bop[0]).getObject());
        }
    }
}

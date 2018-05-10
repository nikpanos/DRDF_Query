/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.test;

import gr.unipi.datacron.common.AppConfig;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.FilterOf;
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.JoinOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.JoinOrOperator;
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
                        "\n" +
                        "SELECT *\n" +
                        "WHERE\n" +
                        "{\n" +
                        "    ?w a :WeatherCondition\n" +
                        "    ; :windDirectionMin \"77.13083\"\n" +
                        "    ; :windDirectionMax \"77.13083\"\n" +
                        "}\n").getBop();
        
        System.out.println("NumberOfTrees: " + bop.length);
        System.out.println("--------------------------");
        System.out.println(bop[0].toString());
        Column[] cs = ((JoinOrOperator)bop[0]).getColumnJoinPredicate();
        System.out.println(cs.length);
        System.out.println();
        for (Column c : cs) {
            System.out.println(c.getColumnName());
            System.out.println(c.getColumnTypes());
            System.out.println();
            System.out.println();
        }

        for (Column c : bop[0].getArrayColumns()) {
            System.out.println();
            System.out.println(c.getColumnName());
            System.out.println(c.getColumnTypes());
            System.out.println(c.getQueryString());
        }
    }
}

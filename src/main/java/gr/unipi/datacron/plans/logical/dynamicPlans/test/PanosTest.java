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
public class PanosTest {


    public static void main(String args[]) {
//        AppConfig.init("C:\\Users\\nikp\\Desktop\\params.hocon");

        //Long encodedValue = getRedisEncodedValue("a");
        //System.out.println(encodedValue);

        //String decodedValue = getRedisDecodedValue(-5L);
        //System.out.println(decodedValue);

        BaseOperator bop = LogicalPlanner.setSparqlQuery(
                "Prefix : <http://www.datacron-project.eu/datAcron#>\n" +
                        "SELECT *\n" +
                        "WHERE\n" +
                        "{\n" +
                        "    ?ves a ?VesselType ;\n" +
                        "    :has_vesselFixingDeviceType ?device ;\n" +
                        "    :vesselName ?name .\n" +
                        "    ?n :ofMovingObject ?ves ;\n" +
                        "    :hasGeometry ?g ;\n" +
                        "    :hasTemporalFeature ?t ;\n" +
                        "    :hasHeading ?heading ;\n" +
                        "    :hasSpeed ?speed .\n" +
                        "    ?event :occurs ?n .\n" +
                        "    ?n :hasWeatherCondition ?w.\n" +
                        "    ?w :windDirectionMin \"77.13083\"\n" +
                        "}\n").optimized().build().getRoot();

        System.out.println("--------------------------");
        System.out.println(bop.toString());
        /*SparqlColumn[] cs = ((JoinOperator)bop[0]).getColumnJoinPredicate();
        System.out.println(cs.length);
        System.out.println();
        System.out.println(cs[0].getColumnName());
        System.out.println(cs[0].getColumnTypes());
        System.out.println();
        System.out.println(cs[1].getColumnName());
        System.out.println(cs[1].getColumnTypes());
        System.out.println();
        System.out.println();

        /*for (SparqlColumn c : bop[0].getArrayColumns()) {
            System.out.println();
            System.out.println(c.getColumnName());
            System.out.println(c.getColumnTypes());
            System.out.println(c.getQueryString());
        }
        System.out.println(bop[0].toString());*/

        /*Date d = new Date();
        d.setTime(1451636000000L);
        SimpleDateFormat df = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss");
        System.out.println(df.format(d));*/
    }
}

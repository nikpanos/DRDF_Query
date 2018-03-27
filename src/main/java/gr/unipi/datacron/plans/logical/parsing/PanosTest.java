/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.parsing;

import gr.unipi.datacron.common.AppConfig;
import gr.unipi.datacron.plans.logical.parsing.operators.BaseOperator;
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
        AppConfig.init("C:\\Users\\nikp\\Desktop\\params.hocon");

        Long encodedValue = getRedisEncodedValue("a");
        System.out.println(encodedValue);

        String decodedValue = getRedisDecodedValue(-5L);
        System.out.println(decodedValue);

        BaseOperator[] bop = MyOpVisitorBase.newMyOpVisitorBase("SELECT ?x"
                + "WHERE"
                + "{"
                + " ?ves <a> ?VesselType ;"
                + " <:hasFixingDevice> ?device ;"
                + " <:has_vesselMMSI> '244010219' ;"
                + " <:has_vesselName> ?name ."
                + " ?n <:ofMovingObject> ?ves ;"
                + " <:hasGeometry> ?g ;"
                + " <:hasTemporalFeature> ?t ."               
                + " ?g <:hasWKT> ?pos ."
                + " ?t <:TimeStart> ?time ."
                + " ?event <:occurs> ?n ."
                + " ?n <:hasHeading> ?heading ."
                + " ?n <:hasSpeed> ?speed ."                             
                + "}").getBop();
        
        System.out.println("NumberOfTrees: " + bop.length);
        for(BaseOperator b:bop){
            System.out.println("--------------------------");
            System.out.println(bop[0].toString());
        }
    }
}

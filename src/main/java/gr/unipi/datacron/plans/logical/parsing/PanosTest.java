/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.parsing;

import gr.unipi.datacron.plans.logical.parsing.operators.BaseOperator;

/**
 *
 * @author nicholaskoutroumanis
 */
public class PanosTest {

    public static void main(String args[]) {

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

package gr.unipi.datacron.plans.logical.dynamicPlans.test;

import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.LogicalPlanner;

public class TestFunction {
    public static void main(String[] args){
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
                        "    FILTER ( :spatioTemporalBox2D(?n, :shortcutSpatial, :shortcutTemporal, -5, -10)) .\n" +
                        "}").build().getRoot();


        System.out.println("--------------------------");
        System.out.println(bop.toString());
    }
}

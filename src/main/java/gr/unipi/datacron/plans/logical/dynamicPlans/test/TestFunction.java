package gr.unipi.datacron.plans.logical.dynamicPlans.test;

import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.LogicalPlanner;

public class TestFunction {
    public static void main(String args[]){
        BaseOperator bop = LogicalPlanner.setSparqlQuery(
                "PREFIX aGeo: <http://example.org/geo#>\n" +
                        "\n" +
                        "SELECT ?neighbor\n" +
                        "WHERE { ?a aGeo:placeName \"Grenoble\" .\n" +
                        "        ?a aGeo:location ?axLoc .\n" +
                        "        ?a aGeo:location ?ayLoc .\n" +
                        "\n" +
                        "        ?b aGeo:placeName ?neighbor .\n" +
                        "        ?b aGeo:location ?bxLoc .\n" +
                        "        ?b aGeo:location ?byLoc .\n" +
                        "\n" +
                        "        FILTER ( aGeo:distance(?axLoc, ?ayLoc, ?bxLoc, ?byLoc) < 10 ) .\n" +
                        "      }").build().getBop();


        System.out.println("--------------------------");
        System.out.println(bop.toString());
    }
}
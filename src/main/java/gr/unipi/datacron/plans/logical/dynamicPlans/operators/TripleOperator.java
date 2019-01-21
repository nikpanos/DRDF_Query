/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.SparqlColumn;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;

/**
 * @author nicholaskoutroumanis
 */
public class TripleOperator extends BaseOpW0Child {

    private TripleOperator(String subject, String predicate, String object) {
        super();

        SparqlColumn[] c = new SparqlColumn[3];

        if (!subject.substring(0, 1).equals("?")) {
            c[0] = SparqlColumn.newSparqlColumn("Subject", subject, ColumnTypes.SUBJECT);
        } else {
            c[0] = (ColumnWithVariable.newColumnWithVariable("Subject", subject, ColumnTypes.SUBJECT));
        }

        if (!predicate.substring(0, 1).equals("?")) {
            c[1] = SparqlColumn.newSparqlColumn("Predicate", predicate, ColumnTypes.PREDICATE);
        } else {
            c[1] = (ColumnWithVariable.newColumnWithVariable("Predicate", predicate, ColumnTypes.PREDICATE));
        }

        if (!object.substring(0, 1).equals("?")) {
            c[2] = SparqlColumn.newSparqlColumn("Object", object, ColumnTypes.OBJECT);
        } else {
            c[2] = (ColumnWithVariable.newColumnWithVariable("Object", object, ColumnTypes.OBJECT));
        }

        setArrayColumns(c);
    }

    public static TripleOperator newTripleOperator(String subject, String predicate, String object) {
        return new TripleOperator(subject, predicate, object);
    }

    @Override
    protected String toString(String margin) {
        return "";
    }

    @Override
    protected long estimateOutputSize() {
        return -1;
    }

    @Override
    public String toString() {
        return "";
    }
}

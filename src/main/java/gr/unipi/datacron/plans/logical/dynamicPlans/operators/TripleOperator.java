/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;

/**
 *
 * @author nicholaskoutroumanis
 */
public class TripleOperator extends BaseOpW0Child {

    private TripleOperator(String subject, String predicate, String object) {
        
        Column[] c = new Column[3];

        if (!subject.substring(0, 1).equals("?")) {
            c[0] = Column.newColumn("Subject", subject, ColumnTypes.SUBJECT);
        } else {
            c[0] = (ColumnWithVariable.newColumnWithVariable("Subject", subject, ColumnTypes.SUBJECT));
        }

        if (!predicate.substring(0, 1).equals("?")) {
            c[1] = Column.newColumn("Predicate", predicate, ColumnTypes.PREDICATE);
        } else {
            c[1] = (ColumnWithVariable.newColumnWithVariable("Predicate", predicate, ColumnTypes.PREDICATE));
        }

        if (!object.substring(0, 1).equals("?")) {
            c[2] = Column.newColumn("Object", object, ColumnTypes.OBJECT);
        } else {
            c[2] = (ColumnWithVariable.newColumnWithVariable("Object", object, ColumnTypes.OBJECT));
        }
        
        setArrayColumns(c);
    }

    public static TripleOperator newTripleOperator(String subject, String predicate, String object) {
        return new TripleOperator(subject, predicate, object);
    }
    
    @Override
    protected String toString(String margin){
        return "";
    }
    
    @Override
    public String toString(){
        return "";
    }  
}

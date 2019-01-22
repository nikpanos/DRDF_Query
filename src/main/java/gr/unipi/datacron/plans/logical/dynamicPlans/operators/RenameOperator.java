/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.SparqlColumn;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author nicholaskoutroumanis
 */
public class RenameOperator extends BaseOpW1Child {

    private final Map<SparqlColumn, SparqlColumn> m;

    private RenameOperator(BaseOperator bo, Map<SparqlColumn, SparqlColumn> m) {
        super(bo);
        this.m = m;
        setArrayColumns(m.values().stream().toArray(SparqlColumn[]::new));
    }

    public static RenameOperator newRenameOperator(BaseOperator bo, Map<SparqlColumn, SparqlColumn> m) {
        return new RenameOperator(bo, m);
    }

    /*@Override
    protected String toString(String margin) {
        return "";
    }*/

    @Override
    protected long estimateOutputSize() {
        return -1;
    }

    /*@Override
    public String toString() {
        return "";
    }*/

    @Override
    protected void addHeaderStringToStringBuilder(StringBuilder builder) {
        builder.append("MAPPING: [");
        Iterator<Map.Entry<SparqlColumn, SparqlColumn>> iter = m.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<SparqlColumn, SparqlColumn> e = iter.next();
            builder.append("KEY(").append(e.getKey()).append("), VALUE(").append(e.getValue()).append(")");
            if (iter.hasNext()) {
                builder.append(", ");
            }
        }
        builder.append(']');
    }

    public Map<SparqlColumn, SparqlColumn> getColumnMapping() {
        return m;
    }

}

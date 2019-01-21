package gr.unipi.datacron.plans.logical.dynamicPlans.operands;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.SparqlColumn;

public class ColumnOperand extends BaseOperand {

    private final SparqlColumn column;

    protected ColumnOperand(SparqlColumn column) {
        this.column = column;

    }

    public static ColumnOperand newColumnOperand(SparqlColumn column) {
        return new ColumnOperand(column);
    }

    public SparqlColumn getColumn() {
        return this.column;
    }

}

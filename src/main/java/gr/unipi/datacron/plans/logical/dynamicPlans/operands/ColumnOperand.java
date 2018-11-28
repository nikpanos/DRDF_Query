package gr.unipi.datacron.plans.logical.dynamicPlans.operands;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;

public class ColumnOperand extends BaseOperand {

    private final Column column;

    protected ColumnOperand(Column column) {
        this.column = column;

    }

    public static ColumnOperand newColumnOperand(Column column) {
        return new ColumnOperand(column);
    }

    public Column getColumn() {
        return this.column;
    }

}

package gr.unipi.datacron.plans.logical.dynamicPlans.operands;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes;
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator;

public class ColumnOperand extends BaseOperand {

    private final Column column;

    protected ColumnOperand(Column column) {
        this.column = column;

    }

    public static ColumnOperand newColumnOperand(Column column){
        return  new ColumnOperand(column);
    }

}

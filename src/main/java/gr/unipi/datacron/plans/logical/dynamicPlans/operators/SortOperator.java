package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithDirection;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;

public class SortOperator extends BaseOpW1Child {

    private final ColumnWithDirection[] columnWithDirection;
    //Query.ORDER_ASCENDING = 1, Query.ORDER_DESCENDING = -1

    private SortOperator(BaseOperator bop, ColumnWithDirection[] columnWithDirection, long outputSize) {
        this.addChild(bop);
        this.fillAndFormArrayColumns();
        this.columnWithDirection = columnWithDirection;
        setOutputSize(outputSize);
    }

    public static SortOperator newSortOperator(BaseOperator baseOperator, ColumnWithDirection[] columnWithDirection, long outputSize) {
        return new SortOperator(baseOperator, columnWithDirection, outputSize);
    }

    public ColumnWithDirection[] getColumnWithDirection() {
        return columnWithDirection;
    }

    @Override
    protected String toString(String margin) {
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass().getSimpleName()).append(" OutputSize: " + this.getOutputSize()).append(" RealOutputSize: " + this.getRealOutputSize()).append("\n");

        s.append(margin).append("Sorting: ").append("\n");
        for (ColumnWithDirection c : columnWithDirection) {
            s.append(margin).append("Column With Sorting: ").append(c.getColumn() + " " + ((c.getDirection() == 1) ? "Ascending" : "Descending")).append("\n");
        }

        s.append(margin).append("Array Columns: \n");
        for (Column c : this.getArrayColumns()) {
            if (c instanceof ColumnWithVariable) {
                s.append(margin).append("ColumnName: ").append(c.getColumnName()).append(" ").append(((ColumnWithVariable) c).getVariableName()).append("\n");
            } else {
                s.append(margin).append("ColumnName: ").append(c.getColumnName()).append("\n");
            }
        }



        return s.toString();
    }

    @Override
    public String toString() {
        return this.toString("");
    }
}

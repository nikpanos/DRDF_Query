package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithDirection;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;

public class SortOperator extends BaseOpW1Child {

    private final ColumnWithDirection[] columnWithDirection;
    //Query.ORDER_ASCENDING = 1, Query.ORDER_DESCENDING = -1

    private SortOperator(BaseOperator bop, ColumnWithDirection[] columnWithDirection) {
        super(bop);
        this.fillAndFormArrayColumns();
        this.columnWithDirection = columnWithDirection;
    }

    public static SortOperator newSortOperator(BaseOperator baseOperator, ColumnWithDirection[] columnWithDirection) {
        return new SortOperator(baseOperator, columnWithDirection);
    }

    public ColumnWithDirection[] getColumnWithDirection() {
        return columnWithDirection;
    }

    @Override
    protected String toString(String margin) {
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass().getSimpleName()).append(" OutputSize: ").append(this.getOutputSize()).append(" RealOutputSize: ").append(this.getRealOutputSize()).append("\n");

        s.append(margin).append("Sorting: ").append("\n");
        for (ColumnWithDirection c : columnWithDirection) {
            s.append(margin).append("Column With Sorting: ").append(c.getColumn()).append(" ").append(c.getDirection()).append("\n");
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
    protected long estimateOutputSize() {
        return getChild().getOutputSize();
    }


    @Override
    public String toString() {
        return this.toString("");
    }
}

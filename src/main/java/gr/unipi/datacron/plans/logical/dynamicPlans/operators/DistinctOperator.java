package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;

public class DistinctOperator extends BaseOpW1Child {

    private DistinctOperator(BaseOperator bop, long outputSize) {
        this.addChild(bop);
        this.fillAndFormArrayColumns();
        setOutputSize(outputSize);
    }

    public static DistinctOperator newDistinctOperator(BaseOperator bop, long outputSize) {
        return new DistinctOperator(bop, outputSize);
    }

    @Override
    protected String toString(String margin) {
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass().getSimpleName()).append(" OutputSize: " + this.getOutputSize()).append(" RealOutputSize: " + this.getRealOutputSize()).append("\n");

        s.append(margin).append("Array Columns: \n");
        for (Column c : this.getArrayColumns()) {
            if (c instanceof ColumnWithVariable) {
                s.append(margin).append("ColumnName: ").append(c.getColumnName()).append(" ").append(((ColumnWithVariable) c).getVariableName()).append("\n");
            } else {
                s.append(margin).append("ColumnName: ").append(c.getColumnName()).append("\n");
            }
        }

        this.getBopChildren().forEach((b) -> {
            s.append(b.toString(margin + "|"));
        });
        return s.toString();
    }

    @Override
    public String toString() {
        return this.toString("");
    }

}

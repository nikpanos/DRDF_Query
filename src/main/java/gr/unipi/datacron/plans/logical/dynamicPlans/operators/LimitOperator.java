package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.OperandPair;

public class LimitOperator extends BaseOpW1Child {

    private final int limit;

    private LimitOperator(BaseOperator baseOperator, int limit, long outputSize) {


        this.addChild(baseOperator);
        this.fillAndFormArrayColumns();
        this.limit = limit;

        if (limit <= outputSize) {
            setOutputSize(limit);
        } else {
            setOutputSize(outputSize);
        }

    }

    public int getLimit() {
        return limit;
    }

    public static LimitOperator newLimitOperator(BaseOperator baseOperator, int limit, long outputSize) {
        return new LimitOperator(baseOperator, limit, outputSize);

    }

    @Override
    protected String toString(String margin) {
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass().getSimpleName()).append(" OutputSize: " + this.getOutputSize()).append(" RealOutputSize: " + this.getRealOutputSize()).append("\n");

        s.append(margin).append("Limit: ").append(limit).append("\n");

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

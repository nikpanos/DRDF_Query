package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.SparqlColumn;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;

public class UnionOperator extends BaseOpW2Child {

    private UnionOperator(BaseOperator bop1, BaseOperator bop2) {
        super(bop1, bop2);
        this.fillAndFormArrayColumns();
    }

    public static UnionOperator newUnionOperator(BaseOperator bop1, BaseOperator bop2) {
        return new UnionOperator(bop1, bop2);
    }

    /*@Override
    protected String toString(String margin) {
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass().getSimpleName()).append(" OutputSize: " + this.getOutputSize()).append(" RealOutputSize: " + this.getRealOutputSize()).append("\n");

        s.append(margin).append("Array Columns: \n");
        for (SparqlColumn c : this.getArrayColumns()) {
            if (c instanceof ColumnWithVariable) {
                s.append(margin).append("ColumnName: ").append(c.getColumnName()).append(" ").append(((ColumnWithVariable) c).getVariableName()).append("\n");
            } else {
                s.append(margin).append("ColumnName: ").append(c.getColumnName()).append("\n");
            }
        }

        for(BaseOperator bop : getBopChildren()){
            s.append(bop.toString(margin + "|"));
        }

        return s.toString();
    }*/

    @Override
    protected long estimateOutputSize() {
        return getLeftChild().getOutputSize() + getRightChild().getOutputSize();
    }

    /*@Override
    public String toString() {
        return this.toString("");
    }*/

}

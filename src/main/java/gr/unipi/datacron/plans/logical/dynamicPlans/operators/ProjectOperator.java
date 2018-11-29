package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;

import java.util.List;

public class ProjectOperator extends BaseOpW1Child {

    private String[] variables;

    private ProjectOperator(BaseOperator bop, List<String> variables) {
        super(bop);
        this.fillAndFormArrayColumns();
        this.variables = variables.stream().toArray(String[]::new);
    }

    public static ProjectOperator newProjectOperator(BaseOperator bop, List<String> variables) {
        return new ProjectOperator(bop, variables);

    }

    public String[] getVariables() {
        return variables.clone();
    }

    @Override
    protected String toString(String margin) {
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass().getSimpleName()).append(" OutputSize: " + this.getOutputSize()).append(" RealOutputSize: " + this.getRealOutputSize()).append("\n");

        s.append("Array Columns: \n");

        for (Column c : this.getArrayColumns()) {
            if (c instanceof ColumnWithVariable) {
                s.append(margin).append("ColumnName:").append(c.getColumnName()).append(" ").append(((ColumnWithVariable) c).getVariableName()).append("\n");
            } else {
                s.append(margin).append("ColumnName:").append(c.getColumnName()).append("\n");
            }
        }

        for(BaseOperator bop : getBopChildren()){
            s.append(bop.toString(margin + "|"));
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

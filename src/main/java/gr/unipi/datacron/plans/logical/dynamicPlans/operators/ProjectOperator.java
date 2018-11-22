package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import java.util.List;

public class ProjectOperator extends BaseOpW1Child {

    private List<String> variables;

    private ProjectOperator(List<String> variables, BaseOperator bop){
        this.variables=variables;
        this.addChild(bop);
    }

    public static ProjectOperator newProjectOperator(List<String> variables, BaseOperator bop) {
        return new ProjectOperator(variables,bop);

    }

    public List<String> getVariables() {
        return variables;
    }
}

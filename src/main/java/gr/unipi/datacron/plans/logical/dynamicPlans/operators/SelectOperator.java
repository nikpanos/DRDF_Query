package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import java.util.List;

public class SelectOperator extends BaseOpW1Child {

    private List<String> variables;

    private SelectOperator(List<String> variables, BaseOperator bop){
        this.variables=variables;
        this.addChild(bop);
    }

    public static SelectOperator newSelectOperator(List<String> variables, BaseOperator bop) {
        return new SelectOperator(variables,bop);

    }

    public List<String> getVariables() {
        return variables;
    }
}

package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

public class LimitOperator extends BaseOpW1Child {

    private final int limit;

    public LimitOperator(int limit){
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }
}

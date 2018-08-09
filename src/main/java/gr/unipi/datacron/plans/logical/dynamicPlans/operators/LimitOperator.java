package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

public class LimitOperator extends BaseOpW1Child {

    private final int limit;

    private LimitOperator(int limit){
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }

    public static LimitOperator newJoinOperator(int limit) {
        return new LimitOperator(limit);

    }
}

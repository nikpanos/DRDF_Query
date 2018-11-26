package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

public class LimitOperator extends BaseOpW1Child {

    private final int limit;

    private LimitOperator(BaseOperator baseOperator, int limit){
        this.addChild(baseOperator);
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }

    public static LimitOperator newLimitOperator(BaseOperator baseOperator, int limit) {
        return new LimitOperator(baseOperator,limit);

    }
}

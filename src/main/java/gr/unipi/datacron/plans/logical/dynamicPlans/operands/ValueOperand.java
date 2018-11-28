package gr.unipi.datacron.plans.logical.dynamicPlans.operands;

public class ValueOperand extends BaseOperand {


    private final String value;

    public ValueOperand(String value) {

        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static ValueOperand newValueOperand(String value) {
        return new ValueOperand(value);
    }
}
package gr.unipi.datacron.plans.logical.dynamicPlans.operands;

public class ValueOperand extends BaseLiteralOperand {
    private final String value;

    private ValueOperand(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static ValueOperand newValueOperand(String value) {
        return new ValueOperand(value);
    }

    @Override
    protected void addContentsToStringBuilder(StringBuilder builder) {
        builder.append("VALUE: ").append(value);
    }
}

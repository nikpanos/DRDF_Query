package gr.unipi.datacron.plans.logical.dynamicPlans.operands;

public abstract class BaseOperand {

    protected abstract void addContentsToStringBuilder(StringBuilder builder);

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(this.getClass().getSimpleName()).append('(');
        addContentsToStringBuilder(builder);
        builder.append(')');
        return builder.toString();
    }
}

package gr.unipi.datacron.plans.logical.dynamicPlans.operands;

public class OperandFunction extends BaseOperand {

    private final String functionName;
    private final BaseOperand[] arguments;

    private OperandFunction(String functionName, BaseOperand[] arguments) {
        this.functionName = functionName;
        this.arguments = arguments;
    }

    public static OperandFunction newOperandFunction(String functionName, BaseOperand... arguments){
        return newOperandFunction(functionName, arguments);
    }

    public String getFunctionName() {
        return functionName;
    }

    public BaseOperand[] getArguments() {
        return arguments;
    }

    @Override
    protected void addContentsToStringBuilder(StringBuilder builder) {
        builder.append("FUNCTION: ").append(functionName);
        builder.append(" ARGUMENTS:[");
        for (BaseOperand arg: arguments) {
            builder.append(arg).append(", ");
        }
        builder.append("]");
    }
}

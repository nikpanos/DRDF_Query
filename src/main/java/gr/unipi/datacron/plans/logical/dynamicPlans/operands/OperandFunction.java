package gr.unipi.datacron.plans.logical.dynamicPlans.operands;

public class OperandFunction extends BaseOperand {

    private final String functionName;
    private final BaseOperand[] arguments;

    public OperandFunction(String functionName, BaseOperand[] arguments) {
        this.functionName = functionName;
        this.arguments = arguments;
    }

    public String getFunctionName() {
        return functionName;
    }

    public BaseOperand[] getArguments() {
        return arguments;
    }
}

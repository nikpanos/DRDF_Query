/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.columns;

import gr.unipi.datacron.plans.logical.dynamicPlans.operands.BaseOperand;

/**
 * @author nicholaskoutroumanis
 */
public class OperandPair {

    private final BaseOperand leftOperand;
    private final BaseOperand rightOperand;
    private final ConditionType conditionType;

    private OperandPair(BaseOperand leftOperand, BaseOperand rightOperand, ConditionType conditionType) {
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
        this.conditionType = conditionType;
    }

    public static OperandPair newOperandPair(BaseOperand leftOperand, BaseOperand rightOperand, ConditionType conditionType) {
        return new OperandPair(leftOperand, rightOperand, conditionType);
    }
//
//    @Override
//    public String toString() {
//        return "COLUMN: " + " VALUE: " + value;
//    }

    public BaseOperand getLeftOperand() {
        return leftOperand;
    }

    public BaseOperand getRightOperand() {
        return rightOperand;
    }

    public ConditionType getConditionType() {
        return conditionType;
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;
import gr.unipi.datacron.plans.logical.dynamicPlans.operands.BaseOperand;
import gr.unipi.datacron.plans.logical.dynamicPlans.operands.OperandPair;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author nicholaskoutroumanis
 */
public class SelectOperator extends BaseOpW1Child {

    private final BaseOperand[] operands;//columns with values only


    private SelectOperator(BaseOperator bo, Column[] c, BaseOperand[] operands, long outputSize) {
        super(outputSize, bo);
        setArrayColumns(c);
        this.operands = operands;
    }

    private Column getColumn(ColumnTypes ct) {
        for (Column c : this.getArrayColumns()) {
            if (c.getColumnTypes() == ct) {
                return c;
            }
        }
        try {
            throw new Exception("Can not define Column Type");
        } catch (Exception ex) {
            Logger.getLogger(SelectOperator.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

    public static SelectOperator newSelectOperator(BaseOperator bo, Column[] c, OperandPair[] operandPairList, long outputSize) {
        return new SelectOperator(bo, c, operandPairList, outputSize);
    }

    public boolean isSubjectVariable() {
        Column c = getColumn(ColumnTypes.SUBJECT);
        return (c instanceof ColumnWithVariable);
    }

    public boolean isPredicateVariable() {
        Column c = getColumn(ColumnTypes.PREDICATE);
        return (c instanceof ColumnWithVariable);
    }

    public boolean isObjectVariable() {
        Column c = getColumn(ColumnTypes.OBJECT);
        return (c instanceof ColumnWithVariable);
    }

    public String getSubject() {
        return getColumn(ColumnTypes.SUBJECT).getQueryString();

    }

    public String getPredicate() {
        return getColumn(ColumnTypes.PREDICATE).getQueryString();

    }

    public String getObject() {
        return getColumn(ColumnTypes.OBJECT).getQueryString();

    }

    @Override
    protected String toString(String margin) {
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass().getSimpleName()).append(" OutputSize: " + this.getOutputSize()).append(" RealOutputSize: " + this.getRealOutputSize()).append("\n");

//        for (OperandPair b : operandPairList) {
//            s.append(margin).append("OperandPair: ").append(this.getClass().getSimpleName()).append(" Left Operand: " + b.getLeftOperand().getClass().getSimpleName() + " - ").append("Right Operand: " + b.getRightOperand().getClass().getSimpleName()).append(", ConditionType: " + b.getConditionType().toString()).append("\n");
//        }

        s.append(margin).append("Array Columns: \n");
        for (Column c : this.getArrayColumns()) {
            if (c instanceof ColumnWithVariable) {
                s.append(margin).append("ColumnName: ").append(c.getColumnName()).append(" ").append(((ColumnWithVariable) c).getVariableName()).append("\n");
            } else {
                s.append(margin).append("ColumnName: ").append(c.getColumnName()).append("\n");
            }
        }

        for(BaseOperator bop : getBopChildren()){
            s.append(bop.toString(margin + "|"));
        }

        return s.toString();
    }

    @Override
    protected long estimateOutputSize() {
        return -1;
    }

    @Override
    public String toString() {
        return this.toString("");
    }

    public BaseOperand[] getOperands() {
        return operands;
    }

}

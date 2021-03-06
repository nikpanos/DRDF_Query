/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.SparqlColumn;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;
import gr.unipi.datacron.plans.logical.dynamicPlans.operands.BaseOperand;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author nicholaskoutroumanis
 */
public class SelectOperator extends BaseOpW1Child {

    private final BaseOperand[] operands;//columns with values only

    private SelectOperator(BaseOperator bo, SparqlColumn[] c, BaseOperand[] operands, long outputSize) {
        super(outputSize, bo);
        setArrayColumns(c);
        this.operands = operands;
    }

    private SparqlColumn getColumn(ColumnTypes ct) {
        for (SparqlColumn c : this.getArrayColumns()) {
            if (c.getColumnTypes() == ct) {
                return c;
            }
        }
        try {
            throw new Exception("Can not define SparqlColumn Type");
        } catch (Exception ex) {
            Logger.getLogger(SelectOperator.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

    public static SelectOperator newSelectOperator(BaseOperator bo, SparqlColumn[] c, BaseOperand[] operands, long outputSize) {
        return new SelectOperator(bo, c, operands, outputSize);
    }

    public boolean isSubjectVariable() {
        SparqlColumn c = getColumn(ColumnTypes.SUBJECT);
        return (c instanceof ColumnWithVariable);
    }

    public boolean isPredicateVariable() {
        SparqlColumn c = getColumn(ColumnTypes.PREDICATE);
        return (c instanceof ColumnWithVariable);
    }

    public boolean isObjectVariable() {
        SparqlColumn c = getColumn(ColumnTypes.OBJECT);
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

    /*@Override
    protected String toString(String margin) {
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass().getSimpleName()).append(" OutputSize: " + this.getOutputSize()).append(" RealOutputSize: " + this.getRealOutputSize()).append("\n");

//        for (PairOperand b : operandPairList) {
//            s.append(margin).append("PairOperand: ").append(this.getClass().getSimpleName()).append(" Left Operand: " + b.getLeftOperand().getClass().getSimpleName() + " - ").append("Right Operand: " + b.getRightOperand().getClass().getSimpleName()).append(", ConditionType: " + b.getConditionType().toString()).append("\n");
//        }

        s.append(margin).append("Array Columns: \n");
        for (SparqlColumn c : this.getArrayColumns()) {
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
    }*/

    @Override
    protected long estimateOutputSize() {
        return -1;
    }

    /*@Override
    public String toString() {
        return this.toString("");
    }*/

    @Override
    protected void addHeaderStringToStringBuilder(StringBuilder builder) {
        builder.append("OPERANDS: [");
        for (BaseOperand op: operands) {
            builder.append('(').append(op).append("), ");
        }
        builder.append(']');
    }

    public BaseOperand[] getOperands() {
        return operands;
    }

}

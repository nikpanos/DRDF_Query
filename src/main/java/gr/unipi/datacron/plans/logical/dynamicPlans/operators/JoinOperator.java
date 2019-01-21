/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.SparqlColumn;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ConditionType;
import gr.unipi.datacron.plans.logical.dynamicPlans.operands.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author nicholaskoutroumanis
 */
public class JoinOperator extends BaseOpW2Child {

    private BaseOperand joinOperand;

    /**
     * @return the columnJoinPredicate
     */
    public BaseOperand getJoinOperand() {
        return joinOperand;
    }

    private JoinOperator(BaseOperator bo1, BaseOperator bo2) {
        super(bo1, bo2);
        this.fillAndFormArrayColumns();
    }

    private JoinOperator(BaseOperator bo1, BaseOperator bo2, BaseOperand joinOperand) {
        super(bo1, bo2);
        this.joinOperand = joinOperand;
    }

    @Override
    protected List<SparqlColumn> formArrayColumns(List<SparqlColumn> columnList) {

        List<Integer> elementsToBeDeleted = new ArrayList<>();
        List<ColumnOperand> columnJoinPredicateList = new ArrayList<>();

        int b;
        int c = 0;
        //find the common variable of the first two child operators        
        for (SparqlColumn i : getBopChildren()[0].getArrayColumns()) {
            if (c == 1) {
                break;
            }
            if (i instanceof ColumnWithVariable) {
                b = 0;
                for (SparqlColumn k : getBopChildren()[1].getArrayColumns()) {
                    if (k instanceof ColumnWithVariable) {
                        if (((ColumnWithVariable) i).getVariableName().equals(((ColumnWithVariable) k).getVariableName())) {
                            elementsToBeDeleted.add(getBopChildren()[0].getArrayColumns().length + b);

                            columnJoinPredicateList.add(ColumnOperand.newColumnOperand(i));
                            columnJoinPredicateList.add(ColumnOperand.newColumnOperand(k));

                            c = 1;
                            break;
                        }
                    }
                    b++;
                }
            }
        }

//        if (c == 0) {
//            try {
//                throw new Exception("The triplets can not be determined by Join Operator");
//            } catch (Exception ex) {
//                Logger.getLogger(JoinOperator.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }

        joinOperand = OperandPair.newOperandPair(columnJoinPredicateList.get(0), columnJoinPredicateList.get(1), ConditionType.EQ);// columnJoinPredicateList.stream().toArray(ColumnOperand[]::new);

        Collections.reverse(elementsToBeDeleted);
        elementsToBeDeleted.forEach((Integer i) -> columnList.remove(i.intValue()));

        return columnList;
    }

    public static JoinOperator newJoinOperator(BaseOperator bo1, BaseOperator bo2) {
        return new JoinOperator(bo1, bo2);
    }

    public static JoinOperator newJoinOperator(BaseOperator bo1, BaseOperator bo2, BaseOperand joinOperand) {
        return new JoinOperator(bo1, bo2, joinOperand);
    }

    private void addOperandStringToStringBuilder(StringBuilder sb, BaseOperand op, String margin) {
        if (op instanceof OperandPair) {
            OperandPair pair = (OperandPair)op;
            addOperandStringToStringBuilder(sb, pair.getLeftOperand(), margin);
            addOperandStringToStringBuilder(sb, pair.getRightOperand(), margin);
        }
        else if (op instanceof ColumnOperand) {
            SparqlColumn c = ((ColumnOperand)op).getColumn();
            sb.append(margin).append("ColumnName: ").append(c.getColumnName()).append(" Variable: ").append(c.getQueryString()).append("\n");
        }
        else if (op instanceof ValueOperand) {
            ValueOperand vo = (ValueOperand)op;
            sb.append(margin).append("Value: ").append(vo.getValue()).append("\n");
        }
        else if (op instanceof ColumnNameOperand) {
            ColumnNameOperand co = (ColumnNameOperand)op;
            sb.append(margin).append("ColumnName: ").append(co.columnName()).append("\n");
        }
        else {
            throw new RuntimeException("This operand type is not supported here: " + op.getClass().getName());
        }
    }

    @Override
    protected String toString(String margin) {
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass().getSimpleName()).append(" OutputSize: ").append(this.getOutputSize()).append(" RealOutputSize: ").append(this.getRealOutputSize()).append("\n");
        s.append(margin).append("Join on Columns: \n");
        addOperandStringToStringBuilder(s, joinOperand, margin);
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
    }

    @Override
    public String toString() {
        return this.toString("");
    }

    @Override
    protected long estimateOutputSize() {

        return getLeftChild().getOutputSize() * getRightChild().getOutputSize();

    }

}

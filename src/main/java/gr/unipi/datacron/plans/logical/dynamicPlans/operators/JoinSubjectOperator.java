/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.SparqlColumn;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author nicholaskoutroumanis
 */
public class JoinSubjectOperator extends BaseOpWnChild {

    private SparqlColumn[] columnJoinPredicate;

    private JoinSubjectOperator(BaseOperator... bo) {
        super(bo);
        this.fillAndFormArrayColumns();
    }

    @Override
    protected List<SparqlColumn> formArrayColumns(List<SparqlColumn> columnList) {

        List<Integer> elementsToBeDeleted = new ArrayList<>();
        List<SparqlColumn> columnJoinPredicateList = new ArrayList<>();

        //first child elements
        ColumnWithVariable a = (ColumnWithVariable) getBopChildren()[0].getArrayColumns()[0];
        columnJoinPredicateList.add(a);

        //other child
        int j = getBopChildren()[0].getArrayColumns().length;
        for (int i = 1; i < getBopChildren().length; i++) {

            ColumnWithVariable c = (ColumnWithVariable) getBopChildren()[i].getArrayColumns()[0];

            //check if subjects of all triplets are the same. if not then throw error
            if (!a.getVariableName().equals(c.getVariableName())) {
                try {
                    throw new Exception("The triplets can not be determined by Join Or Operator");
                } catch (Exception ex) {
                    Logger.getLogger(JoinSubjectOperator.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            columnJoinPredicateList.add(c);
            elementsToBeDeleted.add(j);
            j = j + getBopChildren()[i].getArrayColumns().length;
        }

        columnJoinPredicate = columnJoinPredicateList.stream().toArray(SparqlColumn[]::new);

        Collections.reverse(elementsToBeDeleted);
        elementsToBeDeleted.forEach((Integer i) -> columnList.remove(i.intValue()));

        return columnList;
    }

    public static JoinSubjectOperator newJoinOrOperator(BaseOperator... bo) {
        return new JoinSubjectOperator(bo);
    }

    /**
     * @return the columnJoinPredicate
     */
    public SparqlColumn[] getColumnJoinPredicate() {
        return columnJoinPredicate;
    }

    @Override
    protected String toString(String margin) {
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass().getSimpleName()).append(" OutputSize: " + this.getOutputSize()).append(" RealOutputSize: " + this.getRealOutputSize()).append("\n");
        s.append(margin).append("Join on Columns: \n");
        for (SparqlColumn c : getColumnJoinPredicate()) {
            s.append(margin).append("ColumnName: ").append(c.getColumnName()).append(" Variable: ").append(c.getQueryString()).append("\n");
        }
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

        Long i = Long.MAX_VALUE;
        for (BaseOperator b : getBopChildren()) {
            if (b.getOutputSize() < i) {
                i = b.getOutputSize();
            }

        }

        return i;

    }

}

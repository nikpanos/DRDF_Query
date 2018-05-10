/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author nicholaskoutroumanis
 */
public class JoinOperator extends BaseOpW2Child {

    /**
     * @return the columnJoinPredicate
     */
    public Column[] getColumnJoinPredicate() {
        return columnJoinPredicate;
    }

    private Column[] columnJoinPredicate;

    private JoinOperator(BaseOperator bo1, BaseOperator bo2) {

        this.addChild(bo1,bo2);
        this.fillAndFormArrayColumns();
        setOutputSize(this.estimateOutputSize(bo1,bo2));


    }

    @Override
    protected List<Column> formArrayColumns(List<Column> columnList) {

        List<Integer> elementsToBeDeleted = new ArrayList<>();
        List<Column> columnJoinPredicateList = new ArrayList<>();
         
        //int a = 0;
        int b;
        int c = 0;
        //find the common variable of the first two child operators        
        for (Column i : getBopChildren().get(0).getArrayColumns()) {
            if (c == 1) {
                break;
            }
            if (i instanceof ColumnWithVariable) {
                b = 0;
                for (Column k : getBopChildren().get(1).getArrayColumns()) {
                    if (k instanceof ColumnWithVariable) {
                        if (((ColumnWithVariable) i).getVariableName().equals(((ColumnWithVariable) k).getVariableName())) {
//                            /* first element to be deleted*/
//                            elementsToBeDeleted.add(a);
//                            /*-------*/
                            elementsToBeDeleted.add(getBopChildren().get(0).getArrayColumns().length + b);

                            columnJoinPredicateList.add(i);
                            columnJoinPredicateList.add(k);

                            c = 1;
                            break;
                        }
                    }
                    b++;
                }
            }
            //a++;
        }

        if (c == 0) {
            try {
                throw new Exception("The triplets can not be determined by Join Operator");
            } catch (Exception ex) {
                Logger.getLogger(JoinOperator.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        columnJoinPredicate = columnJoinPredicateList.stream().toArray(Column[]::new);

        Collections.reverse(elementsToBeDeleted);
        elementsToBeDeleted.forEach((Integer i)->columnList.remove(i.intValue()));

        return columnList;
    }

    public static JoinOperator newJoinOperator(BaseOperator bo1, BaseOperator bo2) {
        return new JoinOperator(bo1, bo2);

    }
    
    @Override
    protected String toString(String margin){
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass()).append(" OutputSize: "+this.getOutputSize()).append(" JOIN ON COLUMNS:");
        for(Column c:getColumnJoinPredicate()){
            s.append("ColumnName: ").append(c.getColumnName()).append(" Variable: ").append(c.getQueryString()).append(" ");
        }
        s.append("\n");
        for(Column c:this.getArrayColumns()){
            if(c instanceof ColumnWithVariable){
                s.append(margin).append("ColumnName:").append(c.getColumnName()).append(" ").append(((ColumnWithVariable) c).getVariableName()).append("\n");
            }
            else{
                s.append(margin).append("ColumnName:").append(c.getColumnName()).append("\n");
            }  
        }
        
        this.getBopChildren().forEach((b) -> {
            s.append(b.toString(margin+"|"));
        });
        return s.toString();        
    }
    
    @Override
    public String toString(){
        return this.toString("");
    }

    @Override
    protected long estimateOutputSize(BaseOperator... bo){

        Long i = 1L;
        for (BaseOperator b : bo) {
            i = i * b.getOutputSize();
        }

        return i;

    }

}

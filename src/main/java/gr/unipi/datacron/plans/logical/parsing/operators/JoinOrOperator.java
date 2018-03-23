/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.parsing.operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author nicholaskoutroumanis
 */
public class JoinOrOperator extends BaseOpWnChild {
    
    private Column[] columnJoinPredicate;

    private JoinOrOperator(BaseOperator... bo) {
        this.addChild(bo);
        this.fillAndFormArrayColumns();
    }

    @Override
    protected List<Column> formArrayColumns(List<Column> columnList) {

        List<Integer> elementsToBeDeleted = new ArrayList<>();
        List<Column> columnJoinPredicateList = new ArrayList<>();

        //first child elements
        ColumnWithVariable a = (ColumnWithVariable) getBopChildren().get(0).getArrayColumns()[0];
        columnJoinPredicateList.add(a);
        
        //other child
        int j = getBopChildren().get(0).getArrayColumns().length;
        for (int i = 1; i < getBopChildren().size(); i++) {

            ColumnWithVariable c = (ColumnWithVariable) getBopChildren().get(i).getArrayColumns()[0];

            //check if subjects of all triplets are the same. if not then throw error
            if (!a.getVariableName().equals(c.getVariableName())) {
                try {
                    throw new Exception("The triplets can not be determined by Join Or Operator");
                } catch (Exception ex) {
                    Logger.getLogger(JoinOrOperator.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            columnJoinPredicateList.add(c);
            elementsToBeDeleted.add(j);
            j = j + getBopChildren().get(i).getArrayColumns().length;
        }

        columnJoinPredicate = columnJoinPredicateList.stream().toArray(Column[]::new);

        Collections.reverse(elementsToBeDeleted);
        elementsToBeDeleted.forEach((Integer i)->columnList.remove(i.intValue()));

        return columnList;
    }

    public static JoinOrOperator newJoinOrOperator(BaseOperator... bo) {
        return new JoinOrOperator(bo);
    }
    
    /**
     * @return the columnJoinPredicate
     */
    public Column[] getColumnJoinPredicate() {
        return columnJoinPredicate;
    }

    @Override
    protected String toString(String margin){
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass()).append(" JOIN ON COLUMNS:");
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
    
}

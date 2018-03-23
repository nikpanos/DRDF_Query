/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.parsing.operators;

import java.util.Map;

/**
 *
 * @author nicholaskoutroumanis
 */
public class ProjectOperator extends BaseOpW1Child {
    
    private final Map<Column,Column> m;
    
    private ProjectOperator(BaseOperator bo, Map<Column,Column> m){
        this.addChild(bo);
        this.m=m;
        setArrayColumns(m.values().stream().toArray(Column[]::new));
    }
    
    public static ProjectOperator newProjectOperator(BaseOperator bo, Map<Column,Column> m) {
        return new ProjectOperator(bo, m);
    }    
    
    @Override
    protected String toString(String margin){
        return "";
    }
    
    @Override
    public String toString(){
        return "";
    }         
    
}

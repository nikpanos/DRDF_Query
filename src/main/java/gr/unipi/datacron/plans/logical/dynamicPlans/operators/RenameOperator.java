/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;

import java.util.Map;

/**
 *
 * @author nicholaskoutroumanis
 */
public class RenameOperator extends BaseOpW1Child {
    
    private final Map<Column,Column> m;
    
    private RenameOperator(BaseOperator bo, Map<Column,Column> m){
        this.addChild(bo);
        this.m=m;
        setArrayColumns(m.values().stream().toArray(Column[]::new));
    }
    
    public static RenameOperator newRenameOperator(BaseOperator bo, Map<Column,Column> m) {
        return new RenameOperator(bo, m);
    }    
    
    @Override
    protected String toString(String margin){
        return "";
    }
    
    @Override
    public String toString(){
        return "";
    }

    public Map<Column, Column> getColumnMapping() {
        return m;
    }
    
}

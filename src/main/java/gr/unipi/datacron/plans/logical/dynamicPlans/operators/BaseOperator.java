/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author nicholaskoutroumanis
 */
public abstract class BaseOperator {

    private final List<BaseOperator> bopChildren = new ArrayList<>();//list with base operators children
    private BaseOperator parent;
    private long outputSize;


    private Column[] arrayColumns;//only column values

    protected List<Column> formArrayColumns(List<Column> columnList) {
        return columnList;
    }

    protected void addChild(BaseOperator... bop) {
        for (BaseOperator b : bop) {
            if (this.getNumberOfBopChildren() < getMaxNumberOfChildren()) {
                bopChildren.add(b);
            } else {
                throw new UnsupportedOperationException("Not supported."); //To change body of generated methods, choose Tools | Templates.
            }
        }

    }

    protected void fillAndFormArrayColumns() {

        List<Column> listColumns = new ArrayList<>();

        for (BaseOperator b : bopChildren) {
            for (Column c : b.getArrayColumns()) {
                listColumns.add(c);
            }
        }
        arrayColumns = formArrayColumns(listColumns).stream().toArray(Column[]::new);
    }

    abstract protected int getMaxNumberOfChildren();

    public List<BaseOperator> getBopChildren() {
        return bopChildren;
    }

    public int getNumberOfBopChildren() {
        return bopChildren.size();

    }

    /**
     * @return the arrayColumns
     */
    public Column[] getArrayColumns() {
        return arrayColumns;
    }

    /**
     * @param arrayColumns the arrayColumns to set
     */
    public void setArrayColumns(Column[] arrayColumns) {
        this.arrayColumns = arrayColumns;
    }

    public boolean hasCommonVariable(BaseOperator bop) {
        for (Column c1 : this.arrayColumns) {
            if (c1 instanceof ColumnWithVariable) {
                for (Column c2 : bop.arrayColumns) {
                    if(c2 instanceof ColumnWithVariable){
                        if(((ColumnWithVariable) c1).getVariableName().equals(((ColumnWithVariable) c2).getVariableName())){
                            return true;
                        }
                    }
                }
            }

        }
        return false;
    }
    
    protected String toString(String margin){
        return margin;
    }


    protected long estimateOutputSize(BaseOperator... bo){
        return 0;
    }


    public long getOutputSize() {
        return outputSize;
    }

    protected void setOutputSize(long outputSize) {
        this.outputSize = outputSize;
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.Column;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author nicholaskoutroumanis
 */
public abstract class BaseOperator {


    private final BaseOperator[] bopChildren;//list with base operators children
    //private BaseOperator parent;
    private final long outputSize;
    private long realOutputSize;


    private Column[] arrayColumns;//only column values

    protected List<Column> formArrayColumns(List<Column> columnList) {
        return columnList;
    }

    protected BaseOperator(BaseOperator... bop) {
        this(-1, bop);

    }

    protected BaseOperator(long outputSize, BaseOperator... bop) {
        this.bopChildren = bop;
        if (outputSize == -1) {
            this.outputSize = this.estimateOutputSize();
        }
        else {
            this.outputSize = outputSize;
        }

    }

    /*protected void addChild(BaseOperator... bop) {

    }*/

    protected void fillAndFormArrayColumns() {

        List<Column> listColumns = new ArrayList<>();

        for (BaseOperator b : bopChildren) {
            for (Column c : b.getArrayColumns()) {
                listColumns.add(c);
            }
        }
        arrayColumns = formArrayColumns(listColumns).stream().toArray(Column[]::new);
    }



    public BaseOperator[] getBopChildren() {
        return bopChildren;
    }

    public int getNumberOfBopChildren() {
        return bopChildren.length;

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
                    if (c2 instanceof ColumnWithVariable) {
                        if (((ColumnWithVariable) c1).getVariableName().equals(((ColumnWithVariable) c2).getVariableName())) {
                            return true;
                        }
                    }
                }
            }

        }
        return false;
    }

    protected String toString(String margin) {
        return margin;
    }


    abstract protected long estimateOutputSize(); //{
        //return 0;
    //}


    public long getOutputSize() {
        return outputSize;
    }

    //protected void setOutputSize(long outputSize) {
    //    this.outputSize = outputSize;
    //}

    public long getRealOutputSize() {
        return realOutputSize;
    }

    public void setRealOutputSize(long realOutputSize) {
        this.realOutputSize = realOutputSize;
    }

    protected void setNewChild(BaseOperator child, int childNo) {
        this.bopChildren[childNo] = child;
    }
}

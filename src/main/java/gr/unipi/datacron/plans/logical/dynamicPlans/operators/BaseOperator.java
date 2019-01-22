/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.SparqlColumn;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnWithVariable;

import java.util.ArrayList;
import java.util.List;

/**
 * @author nicholaskoutroumanis
 */
public abstract class BaseOperator {


    private final BaseOperator[] bopChildren;//list with base operators children
    //private BaseOperator parent;
    private final long outputSize;
    private long realOutputSize;


    private SparqlColumn[] arrayColumns;//only column values

    protected List<SparqlColumn> formArrayColumns(List<SparqlColumn> columnList) {
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

        List<SparqlColumn> listColumns = new ArrayList<>();

        for (BaseOperator b : bopChildren) {
            for (SparqlColumn c : b.getArrayColumns()) {
                listColumns.add(c);
            }
        }
        arrayColumns = formArrayColumns(listColumns).stream().toArray(SparqlColumn[]::new);
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
    public SparqlColumn[] getArrayColumns() {
        return arrayColumns;
    }

    /**
     * @param arrayColumns the arrayColumns to set
     */
    public void setArrayColumns(SparqlColumn[] arrayColumns) {
        this.arrayColumns = arrayColumns;
    }

    public boolean hasCommonVariable(BaseOperator bop) {
        for (SparqlColumn c1 : this.arrayColumns) {
            if (c1 instanceof ColumnWithVariable) {
                for (SparqlColumn c2 : bop.arrayColumns) {
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

    protected void addHeaderStringToStringBuilder(StringBuilder builder) {
    }

    private void addNodeToStringBuilder(StringBuilder builder, BaseOperator node, String margin) {
        builder.append(margin).append(node.getClass().getSimpleName()).append(" ");
        node.addHeaderStringToStringBuilder(builder);
        builder.append(" ").append("Columns: [");
        for (SparqlColumn c : node.arrayColumns) {
            builder.append(c);
        }
        builder.append("]\n");
        if (node.bopChildren != null) {
            String newMargin = margin + "|";
            for (BaseOperator childNode : node.bopChildren) {
                addNodeToStringBuilder(builder, childNode, newMargin);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        addNodeToStringBuilder(builder, this, "");
        return builder.toString();
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
}

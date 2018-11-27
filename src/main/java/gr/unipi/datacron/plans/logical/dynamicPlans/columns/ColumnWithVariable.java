/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.columns;

/**
 * @author nicholaskoutroumanis
 */
public class ColumnWithVariable extends Column {

    private final String variableName;

    private ColumnWithVariable(String columnName, String columnVariable, ColumnTypes columnTypes) {
        super(columnName, columnVariable, columnTypes);
        this.variableName = columnVariable;
    }

    public static ColumnWithVariable newColumnWithVariable(String columnName, String columnVariable, ColumnTypes columnTypes) {
        return new ColumnWithVariable(columnName, columnVariable, columnTypes);
    }

    @Override
    public Column copyToNewObject(String prefix) {
        return new ColumnWithVariable(prefix + "." + this.getColumnName(), this.variableName, this.getColumnTypes());
    }

    /**
     * @return the columnValue
     */
    public String getVariableName() {
        return variableName;
    }

    @Override
    public String toString() {
        return super.toString() + " VARIABLE: " + variableName;
    }
}

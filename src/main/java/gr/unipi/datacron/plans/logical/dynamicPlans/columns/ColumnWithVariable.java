/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.columns;

/**
 * @author nicholaskoutroumanis
 */
public class ColumnWithVariable extends SparqlColumn {

    private final String variableName;

    private ColumnWithVariable(String columnName, String columnVariable, ColumnTypes columnTypes) {
        super(columnName, columnVariable, columnTypes);
        this.variableName = columnVariable;
    }

    public static ColumnWithVariable newColumnWithVariable(String columnName, String columnVariable, ColumnTypes columnTypes) {
        return new ColumnWithVariable(columnName, columnVariable, columnTypes);
    }

    @Override
    public SparqlColumn copyToNewObject(String prefix) {
        return new ColumnWithVariable(prefix + "." + this.getColumnName(), this.variableName, this.getColumnTypes());
    }

    /**
     * @return the columnValue
     */
    public String getVariableName() {
        return variableName;
    }

    @Override
    protected void addToStringContents(StringBuilder builder) {
        super.addToStringContents(builder);
        builder.append(' ').append("VARIABLE: ").append(variableName);
    }
}

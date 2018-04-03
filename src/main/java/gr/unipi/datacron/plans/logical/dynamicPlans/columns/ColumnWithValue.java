/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.columns;

/**
 *
 * @author nicholaskoutroumanis
 */
public class ColumnWithValue {
    
    private final Column column;
    private final String value;
    
    private ColumnWithValue(Column column, String value)
    {      
        this.column = column;
        this.value = value;
    }
    
    public static ColumnWithValue newColumnWithValue(Column column, String value)
    {
        return new ColumnWithValue(column, value);
    }

    /**
     * @return the columnValue
     */
    public String getValue() {
        return value;
    }
    
    @Override
    public String toString() {
        return "COLUMN: " + " VALUE: " + value;
    }

    /**
     * @return the column
     */
    public Column getColumn() {
        return column;
    }
    
}

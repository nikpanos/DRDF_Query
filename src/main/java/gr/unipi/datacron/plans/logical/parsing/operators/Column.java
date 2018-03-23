/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.parsing.operators;

/**
 *
 * @author nicholaskoutroumanis
 */
public class Column {

    private final String columnName;
    private final String queryString;
    private final ColumnTypes columnTypes;

    protected Column(String columnName, String queryString, ColumnTypes columnTypes) {
        this.columnName = columnName;
        this.queryString = queryString;
        this.columnTypes = columnTypes;
    }

//    private Column(Column column, int prefix) {
//        columnName = prefix + "." + column.getColumnName();
//    }

    /**
     * @return the queryString
     */
    public String getQueryString() {
        return queryString;
    }
    
    
    public static Column newColumn(String columnName, String queryString, ColumnTypes columnTypes) {
        return new Column(columnName, queryString, columnTypes);
    }

//    public static Column newColumn(Column column, int prefix) {
//        return new Column(column, prefix);
//    }

    public Column copyToNewObject(String prefix) {
        return new Column(prefix+"."+this.columnName, this.getQueryString(), this.getColumnTypes());
    }
    
    /**
     * @return the columnName
     */
    public String getColumnName() {
        return columnName;
    }
    
    /**
     * @return the columnTypes
     */
    public ColumnTypes getColumnTypes() {
        return columnTypes;
    }
    
    public Column getColumn() {
        return this;
    }
    
    @Override
    public String toString() {
        return "COLUMNNAME: " + columnName;
    }
    
}

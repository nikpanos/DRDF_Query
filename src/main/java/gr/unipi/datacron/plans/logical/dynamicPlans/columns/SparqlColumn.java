/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.columns;

/**
 * @author nicholaskoutroumanis
 */
public class SparqlColumn extends Column {
    private final String queryString;
    private final ColumnTypes columnTypes;

    protected SparqlColumn(String columnName, String queryString, ColumnTypes columnTypes) {
        super(columnName);
        this.queryString = queryString;
        this.columnTypes = columnTypes;
    }

//    private SparqlColumn(SparqlColumn column, int prefix) {
//        columnName = prefix + "." + column.getColumnName();
//    }

    /**
     * @return the queryString
     */
    public String getQueryString() {
        return queryString;
    }


    public static SparqlColumn newSparqlColumn(String columnName, String queryString, ColumnTypes columnTypes) {
        return new SparqlColumn(columnName, queryString, columnTypes);
    }

//    public static SparqlColumn newSparqlColumn(SparqlColumn column, int prefix) {
//        return new SparqlColumn(column, prefix);
//    }

    public SparqlColumn copyToNewObject(String prefix) {
        return new SparqlColumn(prefix + "." + this.getColumnName(), this.getQueryString(), this.getColumnTypes());
    }



    /**
     * @return the columnTypes
     */
    public ColumnTypes getColumnTypes() {
        return columnTypes;
    }

    public SparqlColumn getColumn() {
        return this;
    }

    @Override
    protected void addToStringContents(StringBuilder builder) {
        super.addToStringContents(builder);
        builder.append(", ").append("QUERY: ").append(queryString);
        builder.append(", ").append("TYPE: ").append(columnTypes);
    }

}

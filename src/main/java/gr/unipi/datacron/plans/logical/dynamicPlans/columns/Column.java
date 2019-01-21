package gr.unipi.datacron.plans.logical.dynamicPlans.columns;

public class Column {
    private final String columnName;

    protected Column(String columnName) {
        this.columnName = columnName;
    }

    /**
     * @return the columnName
     */
    public String getColumnName() {
        return columnName;
    }

    @Override
    public String toString() {
        return "COLUMNNAME: " + columnName;
    }

    public static Column newColumn(String columnName) {
        return new Column(columnName);
    }
}

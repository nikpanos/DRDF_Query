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

    protected void addToStringContents(StringBuilder builder) {
        builder.append("COLUMNNAME: ").append(columnName);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(this.getClass().getSimpleName()).append('(');
        addToStringContents(builder);
        builder.append(')');
        return builder.toString();
    }

    public static Column newColumn(String columnName) {
        return new Column(columnName);
    }
}

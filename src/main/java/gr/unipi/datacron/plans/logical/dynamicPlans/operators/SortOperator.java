package gr.unipi.datacron.plans.logical.dynamicPlans.operators;

import org.apache.jena.query.Query;

public class SortOperator extends BaseOpW1Child {

    private final String columnName;
    private final int direction;//Query.ORDER_ASCENDING = 1, Query.ORDER_DESCENDING = -1

    private SortOperator(String columnName, int direction){
        this.columnName = columnName;
        this.direction = direction;
    }

    private static SortOperator newSortOperator(String columnName, int direction){
        return new SortOperator(columnName, direction);
    }

    public String getColumnName() {
        return columnName;
    }

    public int getDirection() {
        return direction;
    }
}

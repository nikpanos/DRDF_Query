package gr.unipi.datacron.plans.logical.dynamicPlans.columns;

public class ColumnWithDirection {

    private final SparqlColumn column;
    private final SortDirection direction;

    private ColumnWithDirection(SparqlColumn column, SortDirection direction) {
        this.column = column;
        this.direction = direction;
    }

    public SparqlColumn getColumn() {
        return column;
    }

    public SortDirection getDirection() {
        return direction;
    }

    public static ColumnWithDirection newColumnWithDirection(SparqlColumn column, SortDirection direction) {
        return new ColumnWithDirection(column, direction);
    }
}

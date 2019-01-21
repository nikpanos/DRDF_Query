package gr.unipi.datacron.plans.logical.dynamicPlans.columns;

public class ColumnWithDirection {

    private final Column column;
    private final SortDirection direction;

    private ColumnWithDirection(Column column, SortDirection direction) {
        this.column = column;
        this.direction = direction;
    }

    public Column getColumn() {
        return column;
    }

    public SortDirection getDirection() {
        return direction;
    }

    public static ColumnWithDirection newColumnWithDirection(Column column, SortDirection direction) {
        return new ColumnWithDirection(column, direction);
    }
}

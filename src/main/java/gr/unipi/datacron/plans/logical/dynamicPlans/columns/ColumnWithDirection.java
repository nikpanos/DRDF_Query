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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(this.getClass().getSimpleName());
        builder.append('(').append("COLUMN: ").append(column.getColumnName());
        builder.append(' ').append("DIRECTION: ").append(direction);
        builder.append(')');
        return builder.toString();
    }
}

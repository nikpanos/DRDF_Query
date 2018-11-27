package gr.unipi.datacron.plans.logical.dynamicPlans.columns;

public class ColumnWithDirection {

    private final Column column;
    private final int direction;

    private ColumnWithDirection(Column column, int direction) {
        this.column = column;
        this.direction = direction;
    }

    public Column getColumn() {
        return column;
    }

    public int getDirection() {
        return direction;
    }

    public static ColumnWithDirection newColumnWithDirection(Column column, int direction) {
        return new ColumnWithDirection(column, direction);
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.parsing.operators;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author nicholaskoutroumanis
 */
public class FilterOf extends BaseOpW1Child {

    private final ColumnWithValue[] listofColumnsWithValues;//columns with values only

    private FilterOf(BaseOperator bo, Column[] c, ColumnWithValue[] listofColumnsWithValues) {
        this.addChild(bo);
        setArrayColumns(c);
        this.listofColumnsWithValues = listofColumnsWithValues;
    }

//    private FilterOf(String subject, String predicate, String object) {
//
//        List<ColumnWithValue> columnsWithValues = new ArrayList<>();
//
//        List<Column> arrayColumns = new ArrayList<>();
//
//        if (!subject.substring(0, 1).equals("?")) {
//            Column c = Column.newColumn(this.hashCode() + "." + "Subject");
//            columnsWithValues.add(ColumnWithValue.newColumnWithValue(c, subject));
//            arrayColumns.add(c);
//        } else {
//            arrayColumns.add(ColumnWithVariable.newColumnWithVariable(this.hashCode() + "." + "Subject", subject));
//        }
//
//        if (!predicate.substring(0, 1).equals("?")) {
//            Column c = Column.newColumn(this.hashCode() + "." + "Predicate");
//            columnsWithValues.add(ColumnWithValue.newColumnWithValue(c, predicate));
//            arrayColumns.add(c);
//        } else {
//            arrayColumns.add(ColumnWithVariable.newColumnWithVariable(this.hashCode() + "." + "Predicate", predicate));
//        }
//
//        if (!object.substring(0, 1).equals("?")) {
//            Column c = Column.newColumn(this.hashCode() + "." + "Object");
//            columnsWithValues.add(ColumnWithValue.newColumnWithValue(c, object));
//            arrayColumns.add(c);
//        } else {
//            arrayColumns.add(ColumnWithVariable.newColumnWithVariable(this.hashCode() + "." + "Object", object));
//        }
//
//        listofColumnsWithValues = columnsWithValues.stream().toArray(ColumnWithValue[]::new);
//
//        this.setArrayColumns((Column[]) arrayColumns.stream().toArray(Column[]::new));
//
//    }
    private Column getColumn(ColumnTypes ct) {
        for (Column c : this.getArrayColumns()) {
            if (c.getColumnTypes() == ct) {
                return c;
            }
        }
        try {
            throw new Exception("Can not define Column Type");
        } catch (Exception ex) {
            Logger.getLogger(FilterOf.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

    public static FilterOf newFilterOf(BaseOperator bo, Column[] c, ColumnWithValue[] listofColumnsWithValues) {
        return new FilterOf(bo, c, listofColumnsWithValues);
    }

    public boolean isSubjectVariable() {
        Column c = getColumn(ColumnTypes.SUBJECT);
        return (c instanceof ColumnWithVariable);
    }

    public boolean isPredicateVariable() {
        Column c = getColumn(ColumnTypes.PREDICATE);
        return (c instanceof ColumnWithVariable);
    }

    public boolean isObjectVariable() {
        Column c = getColumn(ColumnTypes.OBJECT);
        return (c instanceof ColumnWithVariable);
    }

    public String getSubject() {
        return getColumn(ColumnTypes.SUBJECT).getQueryString();
//        if (isSubjectVariable()) {
//            return ((ColumnWithVariable) getArrayColumns()[0]).getVariableName();
//        } else {
//            for (ColumnWithValue c : listofColumnsWithValues) {
//                if (c.getColumn().getColumnName().split("\\.")[1].equals("Subject")) {
//                    return c.getValue();
//                }
//            }
//        }
//
//        try {
//            throw new Exception("Can not define the value of Subject");
//
//        } catch (Exception ex) {
//            Logger.getLogger(FilterOf.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return null;

    }

    public String getPredicate() {
        return getColumn(ColumnTypes.PREDICATE).getQueryString();

    }

    public String getObject() {
        return getColumn(ColumnTypes.OBJECT).getQueryString();

    }

    @Override
    protected String toString(String margin) {
        StringBuilder s = new StringBuilder();
        s.append(margin).append("Operator: ").append(this.getClass()).append("\n");
        for (Column c : this.getArrayColumns()) {
            if (c instanceof ColumnWithVariable) {
                s.append(margin).append("ColumnName:").append(c.getColumnName()).append(" ").append(((ColumnWithVariable) c).getVariableName()).append("\n");
            } else {
                s.append(margin).append("ColumnName:").append(c.getColumnName()).append("\n");
            }
        }

        this.getBopChildren().forEach((b) -> {
            s.append(b.toString(margin + "|"));
        });
        return s.toString();
    }

    @Override
    public String toString() {
        return this.toString("");
    }
}

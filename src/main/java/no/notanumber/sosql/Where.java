package no.notanumber.sosql;

public class Where {

    public final DatabaseColumn column;
    public final String operator;
    public final Object value;

    public Where(DatabaseColumn column, String operator) {
        this.column = column;
        this.operator = operator;
        this.value = null;
    }

    public Where(DatabaseColumn column, String operator, Object value) {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }
    
    @Override
    public String toString() {
        return column.table + "." + column.columnName + " " + operator + " " + value;
    }
}

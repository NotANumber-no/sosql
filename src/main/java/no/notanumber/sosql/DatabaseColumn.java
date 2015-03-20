package no.notanumber.sosql;

public class DatabaseColumn {

    public static final String UNDEFINED = "UNDEFINED";

    public final String columnName;

    public final Table table;
    public final Table joinedTo;
    public final ColumnType type;
    public final Class<?> clazz;
    public final int length;

    public DatabaseColumn(String columnName, Class<?> clazz, Table table) {
        this(columnName, clazz, table, 50);
    }

    public DatabaseColumn(String columnName, Class<?> clazz, Table table, int length) {
        this.columnName = columnName;
        this.clazz = clazz;
        this.table = table;
        type = ColumnType.Field;
        this.joinedTo = null;
        this.length = length;
    }

    public DatabaseColumn(String columnName, Class<?> clazz, Table table, ColumnType type) {
        this.columnName = columnName;
        this.clazz = clazz;
        this.table = table;
        this.type = type;
        this.joinedTo = null;
        this.length = 50;
    }
    
    public DatabaseColumn(String columnName, Class<?> clazz, Table table, ColumnType type, Table joinedTo) {
        this.columnName = columnName;
        this.clazz = clazz;
        this.table = table;
        this.type = type;
        this.joinedTo = joinedTo;
        this.length = 50;
    }

}
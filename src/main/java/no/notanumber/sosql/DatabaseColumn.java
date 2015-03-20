package no.notanumber.sosql;

public class DatabaseColumn {

    public static final String UNDEFINED = "UNDEFINED";

    public final String columnName;

    public final String table;
    public final String joinedTo;
    public final ColumnType type;
    public final Class<?> clazz;
    public final int length;

    public DatabaseColumn(String columnName, Class<?> clazz, String table) {
        this(columnName, clazz, table, 50);
    }

    public DatabaseColumn(String columnName, Class<?> clazz, String table, int length) {
        this.columnName = columnName;
        this.clazz = clazz;
        this.table = table;
        type = ColumnType.Field;
        this.joinedTo = null;
        this.length = length;
    }

    public DatabaseColumn(String columnName, Class<?> clazz, String table, ColumnType type) {
        this.columnName = columnName;
        this.clazz = clazz;
        this.table = table;
        this.type = type;
        this.joinedTo = null;
        this.length = 50;
    }
    
    public DatabaseColumn(String columnName, Class<?> clazz, String table, ColumnType type, String joinedTo) {
        this.columnName = columnName;
        this.clazz = clazz;
        this.table = table;
        this.type = type;
        this.joinedTo = joinedTo;
        this.length = 50;
    }

}
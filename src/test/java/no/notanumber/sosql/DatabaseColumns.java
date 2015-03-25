package no.notanumber.sosql;

import java.util.Arrays;
import java.util.List;

public class DatabaseColumns {
    static DatabaseColumn col = new DatabaseColumn("pkey", Long.TYPE, "primary", ColumnType.PrimaryKey);
    static DatabaseColumn withFKey = new DatabaseColumn("fkey", Long.TYPE, "foreign", ColumnType.ForeignKey, "primary");

    @ColumnDefs
    public static List<DatabaseColumn> allColumns() {
        return Arrays.asList(col, withFKey);
    }
}

package no.notanumber.sosql;

public class OrderBy {

    public static final boolean ASCENDING = true;
    public static final boolean DESCENDING = false;

    private final DatabaseColumn column;
    private final boolean asc;

    public OrderBy(DatabaseColumn column) {
        this.column = column;
        this.asc = true;
    }

    public OrderBy(DatabaseColumn column, boolean ascending) {
        this.column = column;
        this.asc = ascending;
    }

    public String getOrder() {
        return asc ? "ASC" : "DESC";
    }

    public DatabaseColumn getColumn() {
        return column;
    }

}

package no.notanumber.sosql;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;
import static no.notanumber.sosql.ColumnType.PrimaryKey;

public class DatabaseColumnHelper {

    private final List<DatabaseColumn> values;

    public DatabaseColumnHelper(List<DatabaseColumn> cols){
        this.values = Collections.unmodifiableList(cols);
    }

    public Optional<DatabaseColumn> getForeignKey(Table primaryKeyTable, Table foreignKeyTable) {
        return values
                .stream()
                .filter(col -> col.joinedTo == primaryKeyTable && col.table == foreignKeyTable)
                .findFirst();
    }

    public Optional<DatabaseColumn> getPrimaryKey(Table table) {
        return values
                .stream()
                .filter(c -> (c.type == PrimaryKey && c.table == table))
                .findFirst();
    }

    public Collection<DatabaseColumn> incomingReferenceColumns(Table t) {
        return values.stream()
                .filter(col -> col.type == ColumnType.ForeignKey && col.joinedTo == t)
                .collect(toList());
    }

    public Collection<DatabaseColumn> getColumnsFor(Table t) {
        return values.stream()
                .filter(c -> c.table == t)
                .collect(toList());
    }
}

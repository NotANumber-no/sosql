package no.notanumber.sosql;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static no.notanumber.sosql.ColumnType.PrimaryKey;

public class ColumnHelper {

    static List<DatabaseColumn> columns;
    static List<String> tables;
    static {
        try {
            Properties props = new Properties();
            props.load(DB.class.getResourceAsStream("/so-sql.properties"));
            DB.class.getResourceAsStream("/so-sql.properties").close();
            Class<?> columnDef = Class.forName(props.getProperty("database-columns"));
            Method getColumnsMethod = Arrays.asList(columnDef.getMethods()).stream().filter(m -> m.getAnnotation(ColumnDefs.class) != null).findFirst().get();
            columns = (List<DatabaseColumn>) getColumnsMethod.invoke(null);
            tables = Collections.unmodifiableList(new ArrayList<>(columns.stream().map(c -> c.table).collect(Collectors.toSet())));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Could not find properties file so-sql.properties on classpath");
        }
    }


    public static DatabaseColumn getColumn(Field f) {
        return columns.stream().filter(c-> c.columnName.equals(f.getAnnotation(Column.class).columnName())).findFirst().orElseThrow(()->new IllegalArgumentException("No column named " + f.getAnnotation(Column.class).columnName()));
    }

    public static Optional<DatabaseColumn> getPrimaryKey(String table) {
        return columns
                .stream()
                .filter(c -> (c.type == PrimaryKey && c.table == table))
                .findFirst();
    }

    public static Optional<DatabaseColumn> getForeignKey(String mainString, String manyToMany) {
        return columns
                .stream()
                .filter(col -> col.joinedTo == mainString && col.table == manyToMany)
                .findFirst();
    }

    public static Collection<DatabaseColumn>  incomingReferenceColumns(String table) {
        return columns.stream()
                .filter(col -> col.type == ColumnType.ForeignKey && col.joinedTo == table)
                .collect(toList());
    }

    public static Collection<DatabaseColumn> getColumnsFor(String t) {
        return columns.stream()
                .filter(c -> c.table == t)
                .collect(toList());
    }

    public static List<Field> getMappedFields(Class<?> clazz) {
        List<Field> columnFields = asList(clazz.getDeclaredFields()).stream()
                .filter(f -> f.isAnnotationPresent(Column.class))
                .collect(toList());
        columnFields.forEach(f->f.setAccessible(true));

        if (clazz.getSuperclass() != null) {
            columnFields.addAll(getMappedFields(clazz.getSuperclass()));
        }
        return columnFields;
    }

    public static Set<String> getTables(Class<?> clazz, Where... searchParams) {
        Set<String> tables = getMappedFields(clazz).stream().map(field -> getColumn(field).table).collect(toSet());

        if (searchParams != null) {
            Stream<String> tableStream = asList(searchParams).stream()
                    .map(param -> param.column.table);
            tables.addAll(tableStream.collect(toList()));
        }
        return tables;
    }

    public static <T> Field getPrimaryKeyField(T obj) {
        return getMappedFields(obj.getClass()).stream().filter(f-> getColumn(f).type == ColumnType.PrimaryKey).findFirst().orElseThrow(() -> new IllegalArgumentException(obj.getClass().getName() + " has no primary key defined"));
    }

    public static <T> String getMainTable(T newOne) {
        return getMainTableForClass(newOne.getClass());
    }

    public static <T> String getMainTableForClass(Class<T> clazz) {
        List<String> pks = getMappedFields(clazz).stream()
                .filter(f -> getColumn(f).type == ColumnType.PrimaryKey)
                .map(f -> getColumn(f).table)
                .collect(toList());
        if (pks.size() > 1) {
            throw new IllegalArgumentException("Multiple primary keys are referenced in class " + clazz + ", must specify which table to do the insert on");
        }
        return pks.get(0);
    }

}

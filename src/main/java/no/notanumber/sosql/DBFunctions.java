package no.notanumber.sosql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import java.beans.PropertyVetoException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang.StringUtils.join;

public class DBFunctions {

    public static final DateTimeFormatter YYYY_MM_DD = DateTimeFormatter.ofPattern("yyyyMMdd");
    public static final List<Class<Integer>> INT_TYPES = asList(Integer.class, Integer.TYPE);
    public static final List<Class<Long>> LONG_TYPES = asList(Long.class, Long.TYPE);
    public static final List<Class<Boolean>> BOOL_TYPES = asList(Boolean.class, Boolean.TYPE);
    private static SimpleWeightedGraph<String, JoinEdge> tableGraph = new SimpleWeightedGraph<String, JoinEdge>(JoinEdge.class) {{
        ColumnHelper.tables.forEach(this::addVertex);
        ColumnHelper.columns.stream()
                .filter(col -> col.type == ColumnType.ForeignKey)
                .forEach(col -> {
                    Join join = new Join(ColumnHelper.getPrimaryKey(col.joinedTo).get(), col);
                    JoinEdge joinEdge = new JoinEdge(join);
                    addEdge(col.table, col.joinedTo, joinEdge);

                    /*
                    the child table is linked to DaycareCenter via a foreign key
                    So is the grownup table. The child and grownup tables are linked together
                    via a many-to-many join table. When finding the shortest path from child to
                    grownup, I want it to find the many-to-many link, not the path via
                    daycare center. Without a weighted graph, the two paths would be seen
                    as equally long.  Giving many-to-many edges half the weight of others,
                    leads to it preferring the many-to-many link in this case. As it should.
                     */

                    boolean manyToManyLink = !ColumnHelper.getPrimaryKey(col.table).isPresent();
                    int weight = manyToManyLink ? 1 : 2;
                    setEdgeWeight(joinEdge, weight);
                });
    }};

    public static ComboPooledDataSource pool;
    public static void setupConnectionPool(String connectionString, String username, String password, int maxConnections) {
        try {
            pool = new ComboPooledDataSource();
            pool.setDriverClass("org.postgresql.Driver");
            pool.setJdbcUrl(connectionString);
            pool.setUser(username);
            pool.setPassword(password);
            pool.setInitialPoolSize(maxConnections / 2);
            pool.setMaxPoolSize(maxConnections);
            pool.setMinPoolSize(1);
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection getConnection() {
        try {
            Connection connection = pool.getConnection();
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String makeSelect(Class<?> clazz, Collection<OrderBy> orderBy, Where... whereClause) {
        Set<String> sql = new HashSet<>();
        List<Column> allColumns = ColumnHelper.getMappedFields(clazz).stream().map(field -> field.getAnnotation(Column.class)).collect(toList());
        Stream<Column> noAggregation = allColumns.stream().filter(col -> col.function() == Function.NONE);
        Stream<Column> aggregated = allColumns.stream().filter(col -> col.function() != Function.NONE);

        noAggregation.forEach(col -> sql.add(col.columnName()));
        aggregated.forEach(col -> sql.add(col.function() + "(" + col.columnName() + ") as " + col.columnName()));
        asList(whereClause).forEach(where -> sql.add(where.column.columnName));
        orderBy.forEach(order -> sql.add(order.getColumn().columnName));

        return "SELECT DISTINCT " + join(sql, ", ");
    }

    public static String makeWhere(Collection<Join> joins, Where... whereClause) {
        List<String> whereParts = new ArrayList<>();

        if (whereClause != null) {
            asList(whereClause).forEach(where ->
                    whereParts.add(where.column.columnName + " " + where.operator + (where.value != null ? " ?" : "")));
        }
        if (!joins.isEmpty()) {
            joins.forEach(join -> whereParts.add(join.foreign.columnName + " = " + join.primary.columnName));
        }

        return whereParts.isEmpty() ? "" : "WHERE " + join(whereParts, " AND ");
    }

    public static <T> String makeGroupBy(Class<T> clazz, Collection<OrderBy> orderBy) {
        List<Field> mappedFields = ColumnHelper.getMappedFields(clazz);
        List<String> noAggregation = mappedFields.stream()
                .map(f -> f.getAnnotation(Column.class))
                .filter(col -> col.function() == Function.NONE)
                .map(col -> col.columnName())
                .collect(toList());

        if ((mappedFields.size() == noAggregation.size()) || noAggregation.isEmpty()) return "";

        final List<String> groupBy = new ArrayList<>(noAggregation);
        orderBy.forEach(order -> groupBy.add(order.getColumn().columnName));
        return " GROUP BY " + join(groupBy, ", ");
    }

    public static List<Object> createParameterList( Where[] whereClause) {
        if (whereClause == null) return new ArrayList<>();
        return asList(whereClause).stream()
                .filter(where -> where.value != null)
                .map(where -> where.value)
                .collect(toList());
    }

    public static Collection<Join> findJoins(Collection<String> tables) {
        Set<Join> joins = new HashSet<>();
        Set<Pair<String, String>> alreadyTried = new HashSet<>();
        for (String a : tables) {
            for (String b: tables) {
                if ( a == b || !alreadyTried.add(new Pair<>(a, b))) continue;
                Stream<Join> jonStream = new DijkstraShortestPath<>(tableGraph, a, b).getPathEdgeList()
                        .stream().map(joinEdge -> joinEdge.join);
                joins.addAll(jonStream.collect(toList()));
            }
        }
        return joins;
    }

    private static class JoinEdge extends DefaultWeightedEdge {
        private final Join join;
        public JoinEdge(Join join) {
            this.join = join;
        }
    }

    public static <A, B> Optional<String> findManyToManyString(A from, B to) {
        Collection<Join> joins = findJoins(asList(ColumnHelper.getMainTable(from), ColumnHelper.getMainTable(to)));
        for (Join j : joins) {
            boolean manyToManyLinkString = !ColumnHelper.getPrimaryKey(j.foreign.table).isPresent();
            if (manyToManyLinkString) return Optional.of(j.foreign.table);
        }
        return Optional.empty();
    }

    public static void set(Field f, Object instance, Object newValue) {
        try {
            f.set(instance, newValue);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object get(Field f, Object instance) {
        try {
            return f.get(instance);
        }catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}

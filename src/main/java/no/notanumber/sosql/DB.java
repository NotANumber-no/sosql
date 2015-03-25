package no.notanumber.sosql;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.trim;

public class DB {

    private static final Logger log = LoggerFactory.getLogger(DB.class);

    private DBFunctions dbFunctions;
    private BigBrother bigBrother;
    private Connection connection;
    final List<Runnable> onSuccessActions = new ArrayList<>(); //to be run when transaction completes successfully

    public DB(DBFunctions dbFunctions, BigBrother bigBrother) {
        this.dbFunctions = dbFunctions;
        this.bigBrother = bigBrother;
        this.connection = dbFunctions.getConnection();
    }

    public <T> List<T> select(Class<T> clazz, Where... whereClause) {
        return select(clazz, new ArrayList<>(), whereClause);
    }

    public <T> List<T> select(Class<T> clazz, OrderBy orderBy, Where... whereClause) {
        return select(clazz, asList(orderBy), whereClause);
    }

    public <T> Optional<T> selectOnlyOne(Class<T> clazz, Where... where) {
        List<T> results = select(clazz, where);
        if (results.isEmpty()) return Optional.empty();
        if (results.size() > 1) throw new IllegalArgumentException("Expected only one value, but got " + results.size() + ". " + Arrays.toString(where));
        return Optional.of(results.get(0));
    }

    private <T> List<T> select(Class<T> clazz, List<OrderBy> orderBy, Where... whereClause) {
        String select = DBFunctions.makeSelect(clazz, orderBy, whereClause);

        Set<String> tables = ColumnHelper.getTables(clazz, whereClause);
        Collection<Join> joins = new ArrayList<>();
        if (tables.size() > 1) {
            joins.addAll(dbFunctions.findJoins(tables));
            joins.forEach(join -> {
                tables.add(join.primary.table);
                tables.add(join.foreign.table);});
        }

        String from = " FROM " + join(tables, ", ");
        String where = DBFunctions.makeWhere(joins, whereClause);
        String groupBy = DBFunctions.makeGroupBy(clazz, orderBy);

        List<String> orderByStrings = orderBy.stream().map(by -> by.getColumn().columnName + " " + by.getOrder()).collect(toList());
        String orderByStr = orderBy.isEmpty() ? "" : "ORDER BY " + join(orderByStrings, ",");

        String sql = join(asList(select, from, where, groupBy, orderByStr), " ");
        return runSQL(clazz, sql, DBFunctions.createParameterList(whereClause));
    }

    public void updateOrInsert(String sql, Object... parameters) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            List<Object> paramList = Arrays.asList(parameters);
            addParameters(stmt, paramList);
            stmt.execute();
            logSQL(sql, paramList);
        } catch (RuntimeException e) {
            log.error(sql);
            throw e;
        } catch (Exception e) {
            log.error(sql);
            throw new RuntimeException(e);
        }
    }

    public <T> List<T> runSQL(Class<T> clazz, String sql, List<Object> parameters) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            addParameters(stmt, parameters);
            log.debug(sql);
            try (ResultSet result = stmt.executeQuery()) {
                List<T> list = new ArrayList<>();
                while (result.next()) {
                    T instance = clazz.newInstance();
                    for (Field f : ColumnHelper.getMappedFields(clazz)) {
                        DBFunctions.set(f, instance, getValueFromRS(result, ColumnHelper.getColumn(f)));
                    }
                    list.add(instance);
                }
                return list;
            }
        } catch (RuntimeException e) {
            log.error(sql);
            throw e;
        } catch (Exception e) {
            log.error(sql);
            throw new RuntimeException(e);
        }
    }
    public <T> List<T> select(DatabaseColumn column, Class<T> clazz,  Where... whereClause) {
        return select(column, clazz, new ArrayList<>(), whereClause);
    }

    public <T> List<T> select(DatabaseColumn column, Class<T> clazz, OrderBy orderBy, Where... whereClause) {
        return select(column, clazz, asList(orderBy), whereClause);
    }

    public <T> List<T> select(DatabaseColumn column, Class<T> clazz, List<OrderBy> orderBy, Where... whereClause) {

        if (!column.clazz.isAssignableFrom(clazz)) throw new IllegalArgumentException(column + " is not of type  " + clazz.getSimpleName());
        Set<String> selectThese = new HashSet<>();
        orderBy.forEach(order -> selectThese.add(order.getColumn().columnName));
        selectThese.add(column.columnName);
        String select = "SELECT DISTINCT " + StringUtils.join(selectThese, ", ");
        Set<String> tables = ColumnHelper.getTables(column.clazz, whereClause);
        tables.add(column.table);
        Collection<Join> joins = new ArrayList<>();
        if (tables.size() > 1) {
            joins.addAll(dbFunctions.findJoins(tables));
            joins.forEach(join -> {tables.add(join.primary.table);tables.add(join.foreign.table);});
        }

        String from = " FROM " + join(tables, ",");
        String where = DBFunctions.makeWhere(joins, whereClause);
        List<String> orderByStrings = orderBy.stream().map(by -> by.getColumn().columnName + " " + by.getOrder()).collect(toList());
        String orderByStr = orderBy.isEmpty() ? "" : "ORDER BY " + join(orderByStrings, ",");
        String sql = join(asList(trim(select), trim(from), trim(where), trim(orderByStr)), " ") .replace("  ", " ");
        log.debug(sql);
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            addParameters(stmt, DBFunctions.createParameterList(whereClause));
            try (ResultSet result = stmt.executeQuery()) {
                List<T> list = new ArrayList<>();
                while (result.next()) {
                    list.add((T)getValueFromRS(result, column));
                }
                return list;
            }
        } catch (RuntimeException e) {
            log.error(sql);
            throw e;
        } catch (Exception e) {
            log.error(sql);
            throw new RuntimeException(e);
        }
    }

    public <T> void update(T updated) {
        update(updated, ColumnHelper.getMainTable(updated));
    }

    public <T> void update(final T updated, String table) {
        List<Field> all = ColumnHelper.getMappedFields(updated.getClass());

        Optional<Field> versionField = all.stream().filter(f -> ColumnHelper.getColumn(f).type == ColumnType.Version).findFirst();
        versionField.ifPresent(field -> {if (DBFunctions.get(field, updated) == null) DBFunctions.set(field, updated, 0);});

        List<Field> inMainString = all.stream()
                .filter(f -> ColumnHelper.getColumn(f).table == table)
                .filter(f -> ColumnHelper.getColumn(f).type != ColumnType.PrimaryKey)
                .collect(toList());
        List<String> setExpressions = inMainString.stream()
                .map(f -> ColumnHelper.getColumn(f).columnName + " = ?")
                .collect(Collectors.toList());
        List<Object> params = inMainString.stream()
                .map(f -> (ColumnHelper.getColumn(f).type == ColumnType.Version) ? (1 + (int) DBFunctions.get(f, updated)) : DBFunctions.get(f, updated))
                .collect(toList());

        Field pk = all.stream().filter(f -> ColumnHelper.getColumn(f).type == ColumnType.PrimaryKey).findFirst()
                .orElseThrow(() -> new IllegalStateException("Cannot update " + updated.getClass().getName() + " as it has no primary key field"));

        String pkName = ColumnHelper.getColumn(pk).columnName;
        String sql = "UPDATE " + table + " SET " + join(setExpressions, ", ") + " WHERE " + pkName + " = " + DBFunctions.get(pk, updated);
        if (versionField.isPresent()) {
            String versionName = ColumnHelper.getColumn(versionField.get()).columnName;
            sql += " AND " + versionName + " = " + DBFunctions.get(versionField.get(), updated);
        }


        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            addParameters(stmt, params);
            if (stmt.executeUpdate() == 0) {
                String errorMsg = "Could not find row in table " + table + " with " + pkName + " = " + DBFunctions.get(pk, updated);
                if (versionField.isPresent()) {
                    errorMsg += " and " + ColumnHelper.getColumn(versionField.get()) + " = " + DBFunctions.get(versionField.get(), updated);
                    throw new ConcurrentModificationException(errorMsg);
                }
                throw new RuntimeException(errorMsg);
            }
            if (versionField.isPresent()) {
                DBFunctions.set(versionField.get(), updated, (Integer) (DBFunctions.get(versionField.get(), updated)) + 1);
            }
            logSQL(sql, params);
            onSuccessActions.add(() -> bigBrother.informAllAgents(updated));
        } catch (SQLException e) {
            log.error(sql);
            throw new RuntimeException(e);
        }
    }

    private void logSQL(String sql, List<Object> params) {
        for (Object p : params) {
            if (p == null) {
                sql = sql.replaceFirst("\\?", "null");
            } else if (p.getClass().equals(String.class) || p.getClass().isEnum()) {
                sql = sql.replaceFirst("\\?", "'" + String.valueOf(p) + "'");
            } else {
                sql = sql.replaceFirst("\\?", String.valueOf(p));
            }
        }
        log.info(sql);
    }

    public void insert(Object... newOnes) {
        asList(newOnes).forEach(obj -> insert(obj, ColumnHelper.getMainTable(obj)));
    }

    public <T> long insert(final T newInstance, String table) {
        List<Field> all = ColumnHelper.getMappedFields(newInstance.getClass());
        List<Field> inMainString = all.stream()
                .filter(f -> ColumnHelper.getColumn(f).table == table)
                .filter(f -> ColumnHelper.getColumn(f).type != ColumnType.PrimaryKey)
                .collect(toList());
        List<String> fieldNames = inMainString.stream().map(f -> ColumnHelper.getColumn(f).columnName).collect(toList());
        List<String> valueMarkers = fieldNames.stream().map(name -> "?").collect(toList());
        List<Object> params = inMainString.stream()
                .map(f -> DBFunctions.get(f, newInstance))
                .collect(toList());

        String sql = "INSERT INTO " + table + "(" + join(fieldNames, ", ") + ") VALUES(" + join(valueMarkers, ", ") + ")";
        try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            addParameters(stmt, params);
            stmt.execute();
            ResultSet newIdRS = stmt.getGeneratedKeys();

            newIdRS.next();
            long newId = newIdRS.getLong(1);
            Optional<Field> pk = all.stream().filter(f -> ColumnHelper.getColumn(f).type == ColumnType.PrimaryKey).findFirst();
            if (pk.isPresent()) {
                DBFunctions.set(pk.get(), newInstance, newId);
            }
            logSQL(sql, params);
            onSuccessActions.add(() -> bigBrother.informAllAgents(newInstance));
            return newId;
        } catch (Exception e) {
            log.error(sql);
            throw new RuntimeException(e);
        }
    }

    public <T> void delete(Class<T> clazz, Where... where) {
        String table = ColumnHelper.getMainTableForClass(clazz);
        Collection<T> deleted = select(clazz, where);
        Optional<DatabaseColumn> pk = ColumnHelper.getPrimaryKey(table);
        String sql = "DELETE FROM " + table + " " + DBFunctions.makeWhere(new ArrayList<Join>(), where);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            List<Object> params = DBFunctions.createParameterList(where);
            addParameters(stmt, params);
            stmt.execute();
            logSQL(sql, params);
            onSuccessActions.add(() -> bigBrother.informAllAgents(deleted.toArray()));
        } catch (Exception e) {
            log.error(sql);
            throw new RuntimeException(e);
        }
    }

    /**
     * Joins a with b via a many-to-many-table-entry.
     * Saves you having to create separate java objects representing each simple many-to-many-join.
     * <p/>
     * example:
     * <p/>
     * Child c = new Child(1);
     * GrownUp g = new GrownUp(2);
     * no.notanumber.sosql.DB.link(c,g);
     * will first find a many-to-many-relationship table that links the two tables Child and GrownUp : GrownUpChild.
     * it will then run the following SQL-statement:
     * INSERT INTO GrownUpChild(gcChildId, gcGrownUpId) VALUES(1,2);
     *
     */
    public void link(Object from, Object to) {
        manyToManyOperation(from, to, "INSERT INTO %s (%s, %s) VALUES(?, ?)");
    }

    public void unlink(Object from, Object to) {
        manyToManyOperation(from, to, "DELETE FROM %s WHERE %s = ? AND %s = ?");
    }

    private void manyToManyOperation(Object from, Object to, String sql) {
        String manyToMany = dbFunctions.findManyToManyString(from, to).orElseThrow(()-> new IllegalArgumentException("No mapping table found between " + from + " and " + to));
        DatabaseColumn fkFrom = ColumnHelper.getForeignKey(ColumnHelper.getMainTable(from), manyToMany).orElseThrow(() -> new IllegalArgumentException("no foreign keys found for " + from ));
        DatabaseColumn fkTo = ColumnHelper.getForeignKey(ColumnHelper.getMainTable(to), manyToMany).orElseThrow(() -> new IllegalArgumentException("no foreign keys found for " + to ));
        String filledOut = String.format(sql, manyToMany,fkFrom.columnName, fkTo.columnName);

        try (PreparedStatement stmt = connection.prepareStatement(filledOut)) {
            Long fromId = (Long) ColumnHelper.getPrimaryKeyField(from).get(from);
            Long toId = (Long) ColumnHelper.getPrimaryKeyField(to).get(to);
            stmt.setLong(1, fromId);
            stmt.setLong(2, toId);
            stmt.executeUpdate();
            logSQL(filledOut, Arrays.asList(fromId, toId));
            onSuccessActions.add(() -> bigBrother.inform(new BigBrother.RowIdentifier(fkFrom, fromId), to));
            onSuccessActions.add(() -> bigBrother.inform(new BigBrother.RowIdentifier(fkTo, toId), from));

        } catch (Exception e) {
            log.error(sql);
            throw new RuntimeException(e);
        }
    }

    public static void addParameters(PreparedStatement stmt, List<Object> params) {
        try {
            for (int i = 1; i <= params.size(); i++) {
                Object p = params.get(i-1);
                if (p == null) {
                    stmt.setObject(i, null);
                } else if (p instanceof Boolean) {
                    stmt.setString(i, ((boolean)p?"T":"F"));
                } else if (p instanceof String) {
                    stmt.setString(i, (String) p);
                } else if (p instanceof Integer) {
                    stmt.setInt(i, (Integer) p);
                } else if (p instanceof Long) {
                    stmt.setLong(i, (Long) p);
                } else if (p instanceof LocalDate) {
                    stmt.setInt(i, Integer.parseInt(((LocalDate) p).format(DBFunctions.YYYY_MM_DD)));
                } else if (p.getClass().isEnum()) {
                    stmt.setString(i, ((Enum<?>) p).name());
                } else if (p.getClass().equals(byte[].class)) {
                    stmt.setBytes(i, (byte[])p);
                } else {
                    throw new IllegalArgumentException("No mapping found for " + p);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Object getValueFromRS(ResultSet rs, DatabaseColumn col) throws IllegalAccessException, SQLException {
        String colName = col.columnName;
        if (col.clazz == String.class) {
            return rs.getString(colName);
        } else if (DBFunctions.BOOL_TYPES.contains(col.clazz)) {
            boolean boolValue = "T".equals(rs.getString(colName));
            return (rs.wasNull() ? null : boolValue);
        } else if (DBFunctions.INT_TYPES.contains(col.clazz)) {
            int intValue = rs.getInt(colName);
            return (rs.wasNull() ? null : intValue);
        } else if (DBFunctions.LONG_TYPES.contains(col.clazz)) {
            long longValue = rs.getLong(colName);
            return (rs.wasNull() ? null : longValue);
        } else if (col.clazz == LocalDate.class) {
            String dateString = String.valueOf(rs.getInt(colName));
            return (rs.wasNull() ? null : LocalDate.parse(dateString, DBFunctions.YYYY_MM_DD));
        } else if (col.clazz == LocalDateTime.class) {
            Timestamp timestamp = rs.getTimestamp(colName);
            return (rs.wasNull() ? null : timestamp.toLocalDateTime());
        }else if (col.clazz == byte[].class) {
            byte[] array = rs.getBytes(colName);
            return rs.wasNull() ? null : array;
        } else if (col.clazz.isEnum()) {
            String enumVal = trim(rs.getString(colName));
            if (!rs.wasNull()) {
                try {
                    return Enum.valueOf((Class<? extends Enum>) col.clazz, enumVal);
                } catch (Exception e) {
                    log.error("Failed to load " + col.clazz + ": " + e.getMessage());
                }
            }
            return null;
        } else {
            throw new IllegalArgumentException("Damn it, didn't find any type for " + colName + " with type " + col.clazz);
        }
    }

    public void commitAndReleaseConnection() {
        try {
            if (connection == null) return;
            if (connection.isClosed()) return;
            connection.commit();
            onSuccessActions.forEach(runnable -> runnable.run());
            onSuccessActions.clear();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void rollback() {
        try {
            if (connection == null) return;
            if (connection.isClosed()) return;
            connection.rollback();
            onSuccessActions.clear();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() {
        return connection;
    }
}

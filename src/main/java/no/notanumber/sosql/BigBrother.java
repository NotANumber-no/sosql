package no.notanumber.sosql;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BigBrother {

    public static interface Spy { void suspectAltered(Object suspect); }

    private final ConcurrentMap<RowIdentifier, WeakHashMap<Spy, Boolean>> spies = new ConcurrentHashMap<>();
    private List<WeakReference<Spy>> spyRefs = Collections.synchronizedList(new ArrayList<>());
    //Keep track of all weak references created, listen for when they are garbage collected,
    // so the main spy-map doesn't fill up with keys that have no living spies left.
    private final ReferenceQueue<Spy> terminatedSpies = new ReferenceQueue<>();

    public BigBrother() {
        new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        terminatedSpies.remove();
                        spyRefs.removeIf(spy -> spy.get() == null);
                        spies.entrySet().removeIf(entry -> entry.getValue().isEmpty());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
    }

    public Optional<Set<Spy>> getSpies(RowIdentifier key) {
        if (spies.containsKey(key)) {
            return Optional.of(spies.get(key).keySet());
        } else {
            return Optional.empty();
        }
    }

    public void inform(RowIdentifier rowIdentifier, Object suspect) {
        getSpies(rowIdentifier).orElse(new HashSet<>()).forEach(spy -> spy.suspectAltered(suspect));
    }

    public void informAllAgents(Object... suspects) {
        for (Object suspect : suspects) {
            findWhoMightBeInterested(suspect).forEach(row -> inform(row, suspect));
        }
    }

    public void spyOn(Object suspect, Spy spy) {
        for (RowIdentifier key : findRowsToSpyOn(suspect)) {
            if (!spies.containsKey(key)) {
                spies.put(key, new WeakHashMap<>());
            }
            spyRefs.add(new WeakReference<>(spy, terminatedSpies));
            spies.get(key).put(spy, true);
        }
    }

    /**
     * Example:
     * findRowsToSpyOn(<child with child_id=42>) would return the
     * primary key row of the child table: child_id = 42,
     * plus the foreign references referring to this entry. For example
     * the Schedule table's schedule_child_id=42
     *
     */
    public <T> Collection<RowIdentifier> findRowsToSpyOn(T suspect) {
        List<RowIdentifier> keys = new ArrayList<>();
        Field pkField = ColumnHelper.getPrimaryKeyField(suspect);
        Object suspectId = DBFunctions.get(pkField, suspect);
        DatabaseColumn pkColumn = ColumnHelper.getColumn(pkField);
        keys.add(new RowIdentifier(pkColumn, suspectId));
        ColumnHelper.incomingReferenceColumns(pkColumn.table).forEach(col -> keys.add(new RowIdentifier(col, suspectId)));
        return keys;
    }

    /**
     * Example:
     * findWhoMightBeInterested(<child with child_id=23 and child_daycare_id=2>) would return the
     * primary key row of the child table: child_id=23, and would also
     * return rows this child "belongs to". Like the day care center with daycare_id=2.
     * It won't return any other fields than what the type <T> actually has defined.
     * If T is a child-class, but does not specify any link to the daycare center,
     * those listening for updates on the daycare center won't be notified.
     * Could fix this by querying the database from here, will maybe have to do this later,
     * but haven't had the need to so far.
     */
    public <T> Collection<RowIdentifier> findWhoMightBeInterested(T suspect) {
        List<RowIdentifier> keys = new ArrayList<>();
        Field pkField = ColumnHelper.getPrimaryKeyField(suspect);
        Object suspectId = DBFunctions.get(pkField, suspect);
        keys.add(new RowIdentifier(ColumnHelper.getColumn(pkField), suspectId));
        for (Field f : ColumnHelper.getMappedFields(suspect.getClass())){
            DatabaseColumn col = ColumnHelper.getColumn(f);
            if (col.type == ColumnType.ForeignKey){
                keys.add(new RowIdentifier(col, DBFunctions.get(f, suspect)));
            }
        }
        return keys;
    }

    public static class RowIdentifier {

        public final Object value;
        public final DatabaseColumn column;

        public RowIdentifier(DatabaseColumn column, Object value) {
            this.column = column;
            this.value = value;
        }

        @Override
        public String toString() {
            return column.columnName + "." + value;
        }

        @Override
        public boolean equals(Object obj) {
            return obj != null
                    && (obj instanceof RowIdentifier)
                    && column == ((RowIdentifier) obj).column
                    && Objects.equals(value, ((RowIdentifier) obj).value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(column, value);
        }
    }
}

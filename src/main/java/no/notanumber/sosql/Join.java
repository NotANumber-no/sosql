package no.notanumber.sosql;

import java.util.Objects;

public class Join {

    public final DatabaseColumn primary;
    public final DatabaseColumn foreign;

    public Join(DatabaseColumn primary, DatabaseColumn foreign) {
        this.primary = primary;
        this.foreign = foreign;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (!(obj instanceof Join))
            return false;
        Join other = (Join) obj;
        return Objects.equals(primary, other.primary) && Objects.equals(foreign, other.foreign);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primary, foreign);
    }

    @Override
    public String toString() {
        return primary.columnName + " = " + foreign.columnName;
    }
}

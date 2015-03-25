import no.notanumber.sosql.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.List;

public class BigBrotherTest {

    public static class Child {

        @Column(columnName = "child_id")
        public Long child_id;
    }

    Child you = new Child();


    private List<DatabaseColumn> columns = Arrays.asList(new DatabaseColumn("child_id", Long.class, "child", ColumnType.PrimaryKey));
    BigBrother bigBrother = new BigBrother(new DBFunctions("", "", "", 1, columns));
    
    @Before
    public void setUp() {
        you.child_id = 1l;
    }
    
    @Test
    public void removes_entry_when_all_spies_are_dead() throws InterruptedException {
        WeakReference<BigBrother.Spy> spyRef = addSpy();
        Assert.assertEquals(1, bigBrother.getSpies(new BigBrother.RowIdentifier(columns.get(0), 1l)).get().size());
        while (spyRef.get() != null) {
            System.gc();
        }
        Thread.sleep(50);
        Assert.assertFalse(bigBrother.getSpies(new BigBrother.RowIdentifier(columns.get(0), 1l)).isPresent());
    }

    private WeakReference<BigBrother.Spy> addSpy() {
        BigBrother.Spy spy = suspect -> {};
        bigBrother.spyOn(you, spy);
        return new WeakReference<>(spy);
    }

}

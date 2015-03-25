package no.notanumber.sosql;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.ref.WeakReference;

public class BigBrotherTest {

    public static class Child {

        @Column(columnName = "pkey")
        public long pkey;
    }

    Child you = new Child();

    BigBrother bigBrother = new BigBrother();
    
    @Before
    public void setUp() throws Exception{
        you.pkey = 1l;
    }

    @Test
    public void removes_entry_when_all_spies_are_dead() throws InterruptedException {
        WeakReference<BigBrother.Spy> spyRef = addSpy();
        Assert.assertEquals(1, bigBrother.getSpies(new BigBrother.RowIdentifier(DatabaseColumns.col, 1l)).get().size());
        int count = 0;
        while (spyRef.get() != null && count++ < 6) {
            System.gc();
        }
        Assert.assertTrue(count < 6);
        Thread.sleep(50);
        Assert.assertFalse(bigBrother.getSpies(new BigBrother.RowIdentifier(DatabaseColumns.col, 1l)).isPresent());
    }

    private WeakReference<BigBrother.Spy> addSpy() {
        BigBrother.Spy spy = new BigBrother.Spy() {
            @Override
            public void suspectAltered(Object suspect) {

            }};
        bigBrother.spyOn(you, spy);
        return new WeakReference<>(spy);
    }

}

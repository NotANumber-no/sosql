package no.notanumber.sosql;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.lang.reflect.Field;
import java.util.Properties;

public class DBTestRunner extends BlockJUnit4ClassRunner {

    static {
        try {
            Properties props = new Properties();
            props.load(DB.class.getResourceAsStream("/so-sql.properties"));
            DB.class.getResourceAsStream("/so-sql.properties").close();
            DBFunctions.setupConnectionPool(props.getProperty("testdb"), props.getProperty("username"), props.getProperty("password"), 5);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Could not find properties file so-sql.properties on classpath");
        }
    }

    public DBTestRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    public void runChild(FrameworkMethod method, RunNotifier notifier) {
        DB db = new DB();
        try {
            db.getConnection().setAutoCommit(false);
            setDBField(db);
            super.runChild(method, notifier);
        }catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            db.rollback();
        }
    }

    private void setDBField(DB db) throws IllegalAccessException {
        for (Field f : getTestClass().getJavaClass().getDeclaredFields()) {
            if (f.getType().equals(DB.class)) {
                f.setAccessible(true);
                f.set(null, db);
                return;
            }
        }
    }
}

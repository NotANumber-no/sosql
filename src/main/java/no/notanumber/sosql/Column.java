package no.notanumber.sosql;

import java.lang.annotation.*;


@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {

    String columnName() default DatabaseColumn.UNDEFINED;

    Function function() default Function.NONE;
}

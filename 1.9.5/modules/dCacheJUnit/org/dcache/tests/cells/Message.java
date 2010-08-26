package org.dcache.tests.cells;

import java.lang.annotation.*;

/**
 * Annotation used for cell options.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Message
{
    boolean required()    default false;
    int     step()        default 0;
    String  cell();
}

package org.dcache.cells;

import java.lang.annotation.*;

/**
 * Annotation used for cell options.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Option
{
    String  name();
    String  description()  default "";
    String  defaultValue() default "";
    String  unit()         default "";
    boolean required()     default false;
    boolean log()          default true;
}

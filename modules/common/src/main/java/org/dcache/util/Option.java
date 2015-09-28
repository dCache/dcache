package org.dcache.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

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

package dmg.util.command;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a field represents a command option.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Option
{
    /**
     * Name of the option.
     */
    String name();

    /**
     * Help string used to display the usage screen.
     *
     * <p>
     * If this value is empty, the option will not be displayed
     * in the usage screen.
     */
    String usage() default "";

    /**
     * Used on the usage screen as a meta variable to represent the
     * value of this option.
     */
    String metaVar() default "";

    /**
     * Used on the usage screen to describe the syntax of the value
     * of the option.
     *
     * Use the following syntax:
     *
     *    []         optional
     *    |          alternation
     *    ...        repetition
     *    UPPERCASE  value
     *
     * Any other symbol is considered a literal.
     */
    String valueSpec() default "";

    /**
     * Specify that the option is mandatory.
     */
    boolean required() default false;

    /**
     * The separator string used to split elements of array options.
     */
    String separator() default "";

    /**
     * Enumeration of allowed values.
     */
    String[] values() default {};

    /**
     * Category descriptor used to group options in help output.
     */
    String category() default "";
}

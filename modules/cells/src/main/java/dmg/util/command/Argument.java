package dmg.util.command;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a field represents a command argument.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Argument
{
    /**
     * Position of the argument.
     *
     * Negative entries count from the of the argument list, ie -1 is the last
     * argument, -2 is the second to last, and so on.
     *
     * Multi valued properties bound to arguments must always be the last entry.
     */
    int index() default 0;

    /**
     * See {@link dmg.util.command.Option#usage()}.
     */
    String help() default "";

    /**
     * See {@link dmg.util.command.Option#metaVar()}.
     */
    String metaVar() default "";

    /**
     * See {@link dmg.util.command.Option#valueSpec()}.
     */
    String valueSpec() default "";

    /**
     * See {@link dmg.util.command.Option#required()}.
     */
    boolean required() default true;
}

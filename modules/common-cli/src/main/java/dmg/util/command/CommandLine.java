package dmg.util.command;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a field represents the entire command line.
 *
 * Applies to fields of types Args and String. In the former case, the entire
 * argument and option set is injected. In the latter case, the entire command
 * line in textual form is injected.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface CommandLine
{
    /**
     * Whether to allow any option or only those enumerated by @Option annotations.
     */
    boolean allowAnyOption() default false;

    /**
     * See {@link Option#valueSpec()}.
     */
    String valueSpec() default "[-KEY[=VALUE]] ...";

    /**
     * Help string used to display the usage screen.
     *
     * <p>
     * If this value is empty, the option will not be displayed
     * in the usage screen.
     */
    String usage() default "";

    /**
     * Category descriptor used to group options in help output.
     */
    String category() default "";
}

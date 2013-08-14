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
}

package dmg.util.command;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a class represents a command.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Command
{
    /**
     * Name of the command. May consist of multiple white space
     * separated words.
     */
    String name();

    /**
     * Short, single sentence, description of the command, without
     * a trailing period.
     */
    String hint() default "";

    /**
     * Longer, detailed, description of the command.
     */
    String description() default "";

    /**
     * ACL specification of the command.
     */
    String[] acl() default {};
}

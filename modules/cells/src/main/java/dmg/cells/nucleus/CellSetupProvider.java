package dmg.cells.nucleus;

import java.io.PrintWriter;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Classes implementing this interface can participate in the creation
 * and processing of cell setup files. Cell setup files are batch files,
 * which when executed recreate the current settings.
 *
 * <p>A CellSetupProvider should be able to process its own setup commands,
 * although the command processing and setup file generation could be split
 * over multiple implementing classes.
 *
 * <p>The interface provides notification points before and after the
 * setup file is executed. Note that the setup may be executed
 * during normal operation.
 *
 * <p>Note that UniversalSpringCell will invoke the notification
 * methods no matter whether a setup file is actually defined or not.
 */
public interface CellSetupProvider
{
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.TYPE, ElementType.METHOD })
    @interface AffectsSetup {}

    /**
     * Adds cell shell commands for recreating the current setup.
     */
    default void printSetup(PrintWriter pw) {}

    /**
     * Invoked before the setup file is executed.
     */
    default void beforeSetup() {}

    /**
     * Invoked after the setup file has been executed.
     */
    default void afterSetup() {}

    /**
     * Returns a mock object of this setup provider. A mock object
     * can process setup commands and print the setup, but it will
     * have no other side effects.
     *
     * By default it uses the default constructor of the implementing
     * class to create an otherwise uninitialized object. If such an
     * instance would violate the above definitions of a mock object,
     * the default behaviour must be overridden.
     */
    default CellSetupProvider mock()
    {
        try {
            return getClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e){
            throw new RuntimeException(
                    "Bug detected: CellSetupProviders must provide a public default constructor: " + e, e);
        }
    }
}


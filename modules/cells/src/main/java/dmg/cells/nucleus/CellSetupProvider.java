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
 * <p>Setups are processed using a CommandInterpreter configured to recognize
 * both {@code ac_} methods as well as annotated commands. A CellSetupProvider
 * should be able to process its own setup commands, although the command
 * processing and setup file generation could be split over multiple
 * implementing classes.
 *
 * <p>Commands that affect the generated setup in any way should be annotated
 * with @AffectsSetup. This also includes commands that are not written
 * to the generated setup, but still affect the setup in some fashion.
 */
public interface CellSetupProvider
{
    /**
     * CommandInterpreter commands that affect the generated setup in some fashion
     * must be annotated with {@code AffectsSetup}. Execution of such commands may
     * trigger synchronization of the setup with external services.
     *
     * <p>Commands annotated in this fashion will be executed sequentially, although
     * non-annotated commands and messages may still be executed concurrently.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.TYPE, ElementType.METHOD })
    @interface AffectsSetup {}

    /**
     * Adds cell shell commands for recreating the current setup.
     */
    default void printSetup(PrintWriter pw) {}

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


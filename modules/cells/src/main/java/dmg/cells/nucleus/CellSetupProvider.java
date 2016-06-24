package dmg.cells.nucleus;

import java.io.PrintWriter;

/**
 * Classes implementing this interface can participate in the creation
 * and processing of cell setup files. Cell setup files are batch files,
 * which when executed recreate the current settings. A CellSetupProvider
 * should be able to process its own setup commands, although the
 * command processing and setup file generation could be split over
 * multiple implementing classes.
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
}


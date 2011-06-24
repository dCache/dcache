package org.dcache.cells;

import java.io.PrintWriter;

/**
 * Classes implementing this interface can participate in the creation
 * of a cell setup file. Cell setup files are batch files, which when
 * executed recreate the current settings.
 *
 * The interface provides notification points before and after the
 * setup file is executed. Notice that the setup may be executed
 * during normal operation.
 *
 * Notice that UniversalSpringCell will invoke the notification
 * methods no matter whether a setup file is actually defined or not.
 */
public interface CellSetupProvider
{
    /**
     * Adds cell shell commands for recreating the current setup.
     */
    void printSetup(PrintWriter pw);

    /**
     * Invoked before the setup file is executed.
     */
    void beforeSetup();

    /**
     * Invoked after the setup file has been executed.
     */
    void afterSetup();
}


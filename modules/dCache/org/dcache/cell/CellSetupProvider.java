package org.dcache.cell;

import java.io.PrintWriter;

/**
 * Classes implementing this interface can participate in the creation
 * of a cell setup file. Cell setup files are batch files, which when
 * executed recreate the current settings.
 */
public interface CellSetupProvider
{
    /**
     * Adds cell shell commands for recreating the current setup.
     */
    void printSetup(PrintWriter pw);

    /**
     * Invoked during initialisation after the setup file has been or
     * would have been executed. This is called no matter whether a
     * setup file was actually executed or not.
     *
     * This allows for some late initialisation, that cannot happen
     * until the settings have been restored. The Spring init-method
     * is called before the setup file is executed and can thus not be
     * used for this purpose.
     */
    void afterSetupExecuted();
}


package org.dcache.cell;

import java.io.PrintWriter;

/**
 * Classes implementing this interface can participate in the creation
 * of a cell setup file. Cell setup files are batch files, which when
 * executed recreated the current settings.
 */
public interface CellSetupProvider
{
    void save(PrintWriter pw);
}


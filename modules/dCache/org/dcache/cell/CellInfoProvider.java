package org.dcache.cell;

import java.io.PrintWriter;

/**
 * Classes implementing this interface can participate in generating
 * output for the Cell info command.
 */
public interface CellInfoProvider
{
    void getInfo(PrintWriter pw);
}
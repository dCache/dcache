package org.dcache.pool.classic;

import java.io.PrintWriter;
import org.dcache.cells.CellSetupProvider;
import diskCacheV111.util.event.AbstractCacheRepositoryListener;
import diskCacheV111.pools.SpaceSweeper;

/**
 * Abstract base class for space sweepers providing empty
 * implementations of <code>printSetup</code> and
 * <code>afterSetupExecuted</code>.
 */
public abstract class AbstractSpaceSweeper
    extends AbstractCacheRepositoryListener
    implements SpaceSweeper,
               CellSetupProvider
{
    public void printSetup(PrintWriter pw) {}
    public void afterSetupExecuted() {}
}

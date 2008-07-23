// $Id: SpaceSweeper.java,v 1.3 2003-08-03 14:53:41 cvs Exp $

package diskCacheV111.pools ;

import java.io.PrintWriter ;
import diskCacheV111.util.event.CacheRepositoryListener ;
import org.dcache.cell.CellSetupProvider;

public interface SpaceSweeper
    extends CacheRepositoryListener, CellSetupProvider
{
   public void printSetup( PrintWriter pw ) ;
   public long getRemovableSpace() ;
   public long getLRUSeconds() ;

}

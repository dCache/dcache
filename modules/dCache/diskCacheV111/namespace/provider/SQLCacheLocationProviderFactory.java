/*
 * $Id: SQLCacheLocationProviderFactory.java,v 1.1 2005-08-11 08:35:29 tigran Exp $
 */
package diskCacheV111.namespace.provider;

import diskCacheV111.namespace.NameSpaceProvider;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;

public class SQLCacheLocationProviderFactory extends DcacheNameSpaceProviderFactory {

    static private SQLNameSpaceProvider provider = null;

    public synchronized NameSpaceProvider getProvider(Args args, CellNucleus nucleus) throws Exception {

        if ( provider == null ) {
            provider = new SQLNameSpaceProvider(args, nucleus);
        }
        return provider;
    }
}

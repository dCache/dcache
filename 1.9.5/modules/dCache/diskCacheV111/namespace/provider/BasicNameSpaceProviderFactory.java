/*
 * $Id: BasicNameSpaceProviderFactory.java,v 1.1 2005-08-11 08:35:28 tigran Exp $
 */
package diskCacheV111.namespace.provider;

import diskCacheV111.namespace.NameSpaceProvider;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;

public class BasicNameSpaceProviderFactory extends DcacheNameSpaceProviderFactory {

    static private BasicNameSpaceProvider provider = null;

    public synchronized NameSpaceProvider getProvider(Args args, CellNucleus nucleus) throws Exception {

        if ( provider == null ) {
            provider = new BasicNameSpaceProvider(args, nucleus);
        }
        return provider;
    }
}

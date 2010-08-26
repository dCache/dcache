/*
 * $Id: DcacheNameSpaceProviderFactory.java,v 1.1 2005-08-11 08:35:28 tigran Exp $
 */
package diskCacheV111.namespace.provider;

import diskCacheV111.namespace.NameSpaceProvider;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;

/**
 * @author tigran
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
abstract public class DcacheNameSpaceProviderFactory {
    public  synchronized NameSpaceProvider getProvider(Args args, CellNucleus nucleus) throws Exception {
        return null;
    };
}

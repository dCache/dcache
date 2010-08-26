/*
 * $Id: ChimeraNameSpaceProviderFactory.java,v 1.1 2007-06-19 10:06:33 tigran Exp $
 */
package org.dcache.chimera.namespace;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.namespace.provider.DcacheNameSpaceProviderFactory;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;

public class ChimeraNameSpaceProviderFactory extends  DcacheNameSpaceProviderFactory {

    static private ChimeraNameSpaceProvider _provider = null;

    public synchronized NameSpaceProvider getProvider(Args args, CellNucleus nucleus) throws Exception {

        if ( _provider == null ) {
            _provider = new ChimeraNameSpaceProvider(args, nucleus);
        }
        return _provider;
    }


}

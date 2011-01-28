/*
 * $Id: ChimeraNameSpaceProviderFactory.java,v 1.1 2007-06-19 10:06:33 tigran Exp $
 */
package org.dcache.chimera.namespace;

import diskCacheV111.namespace.NameSpaceProvider;
import dmg.util.Args;

public class ChimeraNameSpaceProviderFactory
{
    private static ChimeraNameSpaceProvider _provider;

    public static synchronized NameSpaceProvider getProvider(String args)
        throws Exception
    {
        if (_provider == null) {
            _provider = new ChimeraNameSpaceProvider(new Args(args));
        }
        return _provider;
    }
}

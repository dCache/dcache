/*
 * $Id: BasicNameSpaceProviderFactory.java,v 1.1 2005-08-11 08:35:28 tigran Exp $
 */
package diskCacheV111.namespace.provider;

import diskCacheV111.namespace.NameSpaceProvider;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;

public class BasicNameSpaceProviderFactory
{
    private static BasicNameSpaceProvider provider;

    public static synchronized NameSpaceProvider getProvider(String args)
        throws Exception
    {
        if (provider == null) {
            provider = new BasicNameSpaceProvider(new Args(args));
        }
        return provider;
    }
}

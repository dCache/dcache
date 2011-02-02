/*
 * $Id: SQLCacheLocationProviderFactory.java,v 1.1 2005-08-11 08:35:29 tigran Exp $
 */
package diskCacheV111.namespace.provider;

import diskCacheV111.namespace.NameSpaceProvider;
import dmg.util.Args;

public class SQLCacheLocationProviderFactory
{
    private static SQLNameSpaceProvider provider;

    public static synchronized NameSpaceProvider getProvider(Args args)
        throws Exception
    {
        if (provider == null) {
            provider = new SQLNameSpaceProvider(args);
        }
        return provider;
    }
}

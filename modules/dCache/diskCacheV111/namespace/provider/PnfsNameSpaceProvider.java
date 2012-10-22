package diskCacheV111.namespace.provider;

import dmg.util.Args;

/**
 * Name space provider for PNFS.
 *
 * Implemented as a decorator around BasicNameSpaceProvider. The
 * decorator adds permission checking to the legacy name space
 * provider.
 */
public class PnfsNameSpaceProvider
    extends PermissionHandlerNameSpaceProvider
{
    public PnfsNameSpaceProvider(Args args)
        throws Exception
    {
        super(new BasicNameSpaceProvider(args));
    }
}

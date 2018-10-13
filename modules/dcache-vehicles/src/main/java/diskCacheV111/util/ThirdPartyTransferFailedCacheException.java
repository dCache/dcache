package diskCacheV111.util;

import org.dcache.util.Exceptions;

/**
 * Some unspecified problem when transferring a file between dCache and some
 * remote, third-party storage.
 */
public class ThirdPartyTransferFailedCacheException extends CacheException
{
    private static final long serialVersionUID = 1L;

    public static void checkThirdPartyTransferSuccessful(boolean isSuccessful,
            String template, Object...arguments)
            throws ThirdPartyTransferFailedCacheException
    {
        Exceptions.genericCheck(isSuccessful, ThirdPartyTransferFailedCacheException::new,
                template, arguments);
    }

    public ThirdPartyTransferFailedCacheException(String message)
    {
        super(CacheException.THIRD_PARTY_TRANSFER_FAILED, message);
    }

    public ThirdPartyTransferFailedCacheException(String message, Throwable t)
    {
        super(CacheException.THIRD_PARTY_TRANSFER_FAILED, message, t);
    }
}

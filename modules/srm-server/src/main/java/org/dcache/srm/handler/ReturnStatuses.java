package org.dcache.srm.handler;

import com.google.common.base.Function;

import org.dcache.srm.v2_2.TMetaDataSpace;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLLifetimeReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

public class ReturnStatuses
{
    private static final Function<TSURLReturnStatus,TStatusCode> TSURL_RETURN_STATUS =
            new Function<TSURLReturnStatus, TStatusCode>()
            {
                @Override
                public TStatusCode apply(TSURLReturnStatus returnStatus)
                {
                    return returnStatus.getStatus().getStatusCode();
                }
            };
    private static final Function<TSURLLifetimeReturnStatus,TStatusCode> SURL_LIFETIME_RETURN_STATUS =
            new Function<TSURLLifetimeReturnStatus, TStatusCode>()
            {
                @Override
                public TStatusCode apply(TSURLLifetimeReturnStatus returnStatus)
                {
                    return returnStatus.getStatus().getStatusCode();
                }
            };

    private ReturnStatuses()
    {
    }

    private static <T> TReturnStatus getSummaryReturnStatusForSurls(T[] objects, Function<T, TStatusCode> getStatusCode)
    {
        boolean hasFailure = false;
        boolean hasSuccess = false;
        for (T object : objects) {
            if (getStatusCode.apply(object) == TStatusCode.SRM_SUCCESS) {
                hasSuccess = true;
            } else {
                hasFailure = true;
            }
        }
        return ReturnStatuses.getSummaryReturnStatus(hasFailure, hasSuccess);
    }

    public static TReturnStatus getSummaryReturnStatus(TSURLReturnStatus[] returnStatuses)
    {
        return getSummaryReturnStatusForSurls(returnStatuses, TSURL_RETURN_STATUS);
    }

    static TReturnStatus getSummaryReturnStatus(TMetaDataSpace[] metadataSpaces)
    {
        boolean hasFailure = false;
        boolean hasSuccess = false;
        for (TMetaDataSpace metaDataSpace : metadataSpaces) {
            if (metaDataSpace.getStatus().getStatusCode() == TStatusCode.SRM_SUCCESS ||
                    metaDataSpace.getStatus().getStatusCode() == TStatusCode.SRM_SPACE_LIFETIME_EXPIRED ||
                    metaDataSpace.getStatus().getStatusCode() == TStatusCode.SRM_EXCEED_ALLOCATION) {
                hasSuccess = true;
            } else {
                hasFailure = true;
            }
        }
        if (!hasFailure) {
            return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
        } else if (!hasSuccess) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, "The operation failed for all spaces");
        } else {
            return new TReturnStatus(TStatusCode.SRM_PARTIAL_SUCCESS, "The operation failed for some spaces");
        }
    }

    public static TReturnStatus getSummaryReturnStatus(boolean hasFailure, boolean hasSuccess)
    {
        if (!hasFailure) {
            return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
        } else if (!hasSuccess) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, "The operation failed for all SURLs");
        } else {
            return new TReturnStatus(TStatusCode.SRM_PARTIAL_SUCCESS, "The operation failed for some SURLs");
        }
    }

    static TReturnStatus getSummaryReturnStatus(TSURLLifetimeReturnStatus[] surlStatus)
    {
        return getSummaryReturnStatusForSurls(surlStatus, SURL_LIFETIME_RETURN_STATUS);
    }
}

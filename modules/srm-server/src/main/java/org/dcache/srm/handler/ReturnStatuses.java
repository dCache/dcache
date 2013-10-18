package org.dcache.srm.handler;

import com.google.common.base.Function;

import org.dcache.srm.v2_2.TMetaDataSpace;
import org.dcache.srm.v2_2.TRequestSummary;
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
    private static final Function<TRequestSummary,TStatusCode> REQUEST_SUMMARY =
            new Function<TRequestSummary, TStatusCode>()
            {
                @Override
                public TStatusCode apply(TRequestSummary requestSummary)
                {
                    return requestSummary.getStatus().getStatusCode();
                }
            };
    private static final Function<TMetaDataSpace,TStatusCode> META_DATA_SPACE =
            new Function<TMetaDataSpace, TStatusCode>()
            {
                @Override
                public TStatusCode apply(TMetaDataSpace metaDataSpace)
                {
                    return metaDataSpace.getStatus().getStatusCode();
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

    public static <T> TReturnStatus getSummaryReturnStatus(T[] objects, Function<T, TStatusCode> getStatusCode)
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

    public static TReturnStatus getSummaryReturnStatus(TRequestSummary[] requestSummaries)
    {
        return getSummaryReturnStatus(requestSummaries, REQUEST_SUMMARY);
    }

    public static TReturnStatus getSummaryReturnStatus(TSURLReturnStatus[] returnStatuses)
    {
        return getSummaryReturnStatus(returnStatuses, TSURL_RETURN_STATUS);
    }

    static TReturnStatus getSummaryReturnStatus(TMetaDataSpace[] metadataSpaces)
    {
        return getSummaryReturnStatus(metadataSpaces, META_DATA_SPACE);
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
        return getSummaryReturnStatus(surlStatus, SURL_LIFETIME_RETURN_STATUS);
    }
}

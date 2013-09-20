package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

public class ReturnStatuses
{
    private ReturnStatuses()
    {
    }

    public static TReturnStatus getSummaryReturnStatus(TSURLReturnStatus[] returnStatuses)
    {
        boolean hasFailure = false;
        boolean hasSuccess = false;
        for (TSURLReturnStatus returnStatus : returnStatuses) {
            if (returnStatus.getStatus().getStatusCode() == TStatusCode.SRM_SUCCESS) {
                hasSuccess = true;
            } else {
                hasFailure = true;
            }
        }
        return getSummaryReturnStatus(hasFailure, hasSuccess);
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
}

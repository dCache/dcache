/*
 * RequestStatusTool.java
 *
 * Created on December 6, 2005, 5:40 PM
 */

package org.dcache.srm.util;

import javax.annotation.Nonnull;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 * @author  timur
 */
public class RequestStatusTool {

    public static final boolean isFailedRequestStatus(@Nonnull TReturnStatus returnStatus)
    {
           TStatusCode statusCode = checkNotNull(returnStatus.getStatusCode());
           return
               statusCode != TStatusCode.SRM_PARTIAL_SUCCESS &&
               statusCode != TStatusCode.SRM_REQUEST_INPROGRESS &&
               statusCode != TStatusCode.SRM_REQUEST_QUEUED &&
               statusCode != TStatusCode.SRM_REQUEST_SUSPENDED &&
               statusCode != TStatusCode.SRM_SUCCESS  &&
               statusCode != TStatusCode.SRM_DONE;

    }

    public static final boolean isFailedFileRequestStatus(@Nonnull TReturnStatus returnStatus)
    {
           TStatusCode statusCode = checkNotNull(returnStatus.getStatusCode());
           return
               statusCode != TStatusCode.SRM_SPACE_AVAILABLE &&
               statusCode != TStatusCode.SRM_FILE_PINNED &&
               statusCode != TStatusCode.SRM_FILE_IN_CACHE &&
               statusCode != TStatusCode.SRM_FILE_PINNED &&
               statusCode != TStatusCode.SRM_SUCCESS &&
               statusCode != TStatusCode.SRM_REQUEST_INPROGRESS &&
               statusCode != TStatusCode.SRM_REQUEST_QUEUED &&
               statusCode != TStatusCode.SRM_REQUEST_SUSPENDED &&
               statusCode != TStatusCode.SRM_DONE;
    }


    public static final boolean isTransientStateStatus(@Nonnull TReturnStatus returnStatus)
    {
           TStatusCode statusCode = checkNotNull(returnStatus.getStatusCode());
           return
               statusCode == TStatusCode.SRM_REQUEST_QUEUED ||
                   statusCode == TStatusCode.SRM_REQUEST_INPROGRESS;
    }


}

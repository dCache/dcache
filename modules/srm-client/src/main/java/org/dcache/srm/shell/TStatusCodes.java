package org.dcache.srm.shell;

import org.dcache.srm.SRMAbortedException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMExceedAllocationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileUnvailableException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMNoFreeSpaceException;
import org.dcache.srm.SRMNonEmptyDirectoryException;
import org.dcache.srm.SRMNotSupportedException;
import org.dcache.srm.SRMOtherException;
import org.dcache.srm.SRMReleasedException;
import org.dcache.srm.SRMRequestTimedOutException;
import org.dcache.srm.SRMSpaceLifetimeExpiredException;
import org.dcache.srm.SRMTooManyResultsException;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static java.util.Arrays.asList;

/**
 * Utility methods for TStatusCode class.
 */
public class TStatusCodes
{
    private TStatusCodes()
    {
        // Utility class: prevent initialisation
    }

    /**
     * Check the status of a bulk operation.  If the result is PARTIAL_SUCCESS
     * or FAILURE then also check the result at the file level.
     */
    public static void checkBulkSuccess(TReturnStatus requestStatus,
            Iterable<TReturnStatus> fileStatuses) throws SRMException
    {
        TStatusCode code = requestStatus.getStatusCode();
        if (code == TStatusCode.SRM_FAILURE || code == TStatusCode.SRM_PARTIAL_SUCCESS) {
            for (TReturnStatus status : fileStatuses) {
                checkSuccess(status);
            }
        } else {
            checkSuccess(requestStatus);
        }
    }

    public static void checkSuccess(TReturnStatus returnStatus) throws SRMException
    {
        checkSuccess(returnStatus, TStatusCode.SRM_SUCCESS);
    }

    public static void checkSuccess(TReturnStatus returnStatus, TStatusCode... success) throws SRMException
    {
        TStatusCode statusCode = returnStatus.getStatusCode();
        String explanation = returnStatus.getExplanation();
        if (asList(success).contains(statusCode)) {
            return;
        }
        if (statusCode == TStatusCode.SRM_FAILURE) {
            throw new SRMException(explanation);
        } else if (statusCode == TStatusCode.SRM_PARTIAL_SUCCESS) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_AUTHENTICATION_FAILURE) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_AUTHORIZATION_FAILURE) {
            throw new SRMAuthorizationException(explanation);
        } else if (statusCode == TStatusCode.SRM_INVALID_REQUEST) {
            throw new SRMInvalidRequestException(explanation);
        } else if (statusCode == TStatusCode.SRM_INVALID_PATH) {
            throw new SRMInvalidPathException(explanation);
        } else if (statusCode == TStatusCode.SRM_FILE_LIFETIME_EXPIRED) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_SPACE_LIFETIME_EXPIRED) {
            throw new SRMSpaceLifetimeExpiredException(explanation);
        } else if (statusCode == TStatusCode.SRM_EXCEED_ALLOCATION) {
            throw new SRMExceedAllocationException(explanation);
        } else if (statusCode == TStatusCode.SRM_NO_USER_SPACE) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_NO_FREE_SPACE) {
            throw new SRMNoFreeSpaceException(explanation);
        } else if (statusCode == TStatusCode.SRM_DUPLICATION_ERROR) {
            throw new SRMDuplicationException(explanation);
        } else if (statusCode == TStatusCode.SRM_NON_EMPTY_DIRECTORY) {
            throw new SRMNonEmptyDirectoryException(explanation);
        } else if (statusCode == TStatusCode.SRM_TOO_MANY_RESULTS) {
            throw new SRMTooManyResultsException(explanation);
        } else if (statusCode == TStatusCode.SRM_INTERNAL_ERROR) {
            throw new SRMInternalErrorException(explanation);
        } else if (statusCode == TStatusCode.SRM_FATAL_INTERNAL_ERROR) {
            throw new SRMInternalErrorException(explanation);
        } else if (statusCode == TStatusCode.SRM_NOT_SUPPORTED) {
            throw new SRMNotSupportedException(explanation);
        } else if (statusCode == TStatusCode.SRM_REQUEST_QUEUED) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_REQUEST_INPROGRESS) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_ABORTED) {
            throw new SRMAbortedException(explanation);
        } else if (statusCode == TStatusCode.SRM_RELEASED) {
            throw new SRMReleasedException(explanation);
        } else if (statusCode == TStatusCode.SRM_FILE_PINNED) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_FILE_IN_CACHE) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_SPACE_AVAILABLE) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_LOWER_SPACE_GRANTED) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_REQUEST_TIMED_OUT) {
            throw new SRMRequestTimedOutException(explanation);
        } else if (statusCode == TStatusCode.SRM_LAST_COPY) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_FILE_BUSY) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_FILE_LOST) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_FILE_UNAVAILABLE) {
            throw new SRMFileUnvailableException(explanation);
        } else if (statusCode == TStatusCode.SRM_CUSTOM_STATUS) {
            throw new SRMOtherException(statusCode, explanation);
        } else {
            throw new SRMOtherException(statusCode, explanation);
        }
    }
}

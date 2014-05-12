package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.net.URI;
import java.util.concurrent.Semaphore;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.RemoveFileCallback;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
import org.dcache.srm.v2_2.SrmRmRequest;
import org.dcache.srm.v2_2.SrmRmResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmRm
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmRm.class);

    private final AbstractStorageElement storage;
    private final SrmRmRequest request;
    private final SRMUser user;
    private final int sizeOfSingleRemoveBatch;
    private SrmRmResponse response;

    public SrmRm(SRMUser user,
                 RequestCredential credential,
                 SrmRmRequest request,
                 AbstractStorageElement storage,
                 SRM srm,
                 String clientHost)
    {
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.storage = checkNotNull(storage);
        this.sizeOfSingleRemoveBatch = srm.getConfiguration().getSizeOfSingleRemoveBatch();
    }

    public SrmRmResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmRm();
            } catch (DataAccessException e) {
                LOGGER.error(e.toString());
                response = getResponse("Internal database failure", TStatusCode.SRM_INTERNAL_ERROR);
            } catch (InterruptedException e) {
                response = getResponse("Operation interrupted", TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMInternalErrorException e) {
                response = getResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMInvalidRequestException e) {
                response = getResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            }
        }
        return response;
    }

    private SrmRmResponse srmRm()
            throws DataAccessException, InterruptedException, SRMInternalErrorException, SRMInvalidRequestException
    {
        if (request.getArrayOfSURLs() == null) {
            throw new SRMInvalidRequestException("arrayOfSURLs is empty");
        }
        org.apache.axis.types.URI[] surls =
                request.getArrayOfSURLs().getUrlArray();
        if (surls == null || surls.length == 0) {
            throw new SRMInvalidRequestException("arrayOfSURLs is empty");
        }

        TSURLReturnStatus[] returnStatuses = new TSURLReturnStatus[surls.length];

        Semaphore semaphore = new Semaphore(sizeOfSingleRemoveBatch);
        for (int i = 0; i < surls.length; i++) {
            semaphore.acquire();
            returnStatuses[i] = new TSURLReturnStatus(surls[i], null);
            URI surl = URI.create(surls[i].toString());
            storage.removeFile(user, surl, new Callback(semaphore, returnStatuses[i]));
        }
        semaphore.acquire(sizeOfSingleRemoveBatch);

        for (int i = 0; i < surls.length; i++) {
            TSURLReturnStatus returnStatus = returnStatuses[i];
            if (returnStatus.getStatus().getStatusCode() == TStatusCode.SRM_INTERNAL_ERROR) {
                throw new SRMInternalErrorException(returnStatus.getStatus().getExplanation());
            }
            if (returnStatus.getStatus().getStatusCode() == TStatusCode.SRM_AUTHORIZATION_FAILURE) {
                continue;
            }

            // [SRM 2.2, 4.3.2, e)] srmRm aborts the SURLs from srmPrepareToPut requests not yet
            // in SRM_PUT_DONE state, and must set its file status as SRM_ABORTED.
            //
            // [SRM 2.2, 4.3.2, f)] srmRm must remove SURLs even if the statuses of the SURLs
            // are SRM_FILE_BUSY. In this case, operations such as srmPrepareToPut or srmCopy
            // that holds the SURL status as SRM_FILE_BUSY must return SRM_INVALID_PATH upon
            // status request or srmPutDone.
            //
            // It seems the SRM specs is undecided about whether to move put requests to
            // SRM_ABORTED or SRM_INVALID_PATH. We choose SRM_ABORTED as it seems like the saner
            // of the two options.
            URI surl = URI.create(surls[i].toString());
            for (PutFileRequest request : SRM.getSRM().getActiveFileRequests(PutFileRequest.class, surl)) {
                try {
                    request.abort("Upload aborted because the file was deleted by another request.");
                    returnStatus.setStatus(new TReturnStatus(TStatusCode.SRM_SUCCESS, "Upload was aborted."));
                } catch (IllegalStateTransition e) {
                    // The request likely aborted or finished before we could abort it
                    LOGGER.debug("srmRm attempted to abort put request {}, but failed: {}",
                            request.getId(), e.getMessage());
                } catch (SRMException e) {
                    returnStatus.setStatus(new TReturnStatus(e.getStatusCode(), e.getMessage()));
                }
            }

            if (returnStatus.getStatus().getStatusCode() != TStatusCode.SRM_SUCCESS) {
                continue;
            }

            // [SRM 2.2, 4.3.2, d)] srmLs,srmPrepareToGet or srmBringOnline must not find these
            // removed files any more. It must set file requests on SURL from srmPrepareToGet
            // as SRM_ABORTED.
            for (GetFileRequest request : SRM.getSRM().getActiveFileRequests(GetFileRequest.class, surl)) {
                try {
                    request.abort("Download aborted because the file was deleted by another request.");
                } catch (IllegalStateTransition e) {
                    // The request likely aborted or finished before we could abort it
                    LOGGER.debug("srmRm attempted to abort get request {}, but failed: {}",
                            request.getId(), e.getMessage());
                } catch (SRMException e) {
                    returnStatus.setStatus(new TReturnStatus(e.getStatusCode(), e.getMessage()));
                }
            }
        }

        return new SrmRmResponse(
                ReturnStatuses.getSummaryReturnStatus(returnStatuses),
                new ArrayOfTSURLReturnStatus(returnStatuses));
    }

    private static class Callback implements RemoveFileCallback
    {
        private final Semaphore semaphore;
        private final TSURLReturnStatus returnStatus;

        public Callback(Semaphore semaphore, TSURLReturnStatus returnStatus)
        {
            this.semaphore = semaphore;
            this.returnStatus = returnStatus;
        }

        @Override
        public void failure(String reason)
        {
            returnStatus.setStatus(new TReturnStatus( TStatusCode.SRM_FAILURE, reason));
            LOGGER.error("RemoveFileFailed: {}", reason);
            done();
        }

        @Override
        public void notFound(String reason)
        {
            returnStatus.setStatus(new TReturnStatus(TStatusCode.SRM_INVALID_PATH, reason));
            done();
        }

        @Override
        public void success()
        {
            returnStatus.setStatus(new TReturnStatus(TStatusCode.SRM_SUCCESS, null));
            done();
        }

        @Override
        public void timeout()
        {
            returnStatus.setStatus(new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR, "Internal timeout"));
            done();
        }

        @Override
        public void permissionDenied()
        {
            returnStatus.setStatus(new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE, "Permission denied"));
            done();
        }

        private void done()
        {
            semaphore.release();
        }
    }

    public static final SrmRmResponse getFailedResponse(String error)
    {
        return getResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmRmResponse getResponse(String error,
                                                  TStatusCode statusCode)
    {
        SrmRmResponse response = new SrmRmResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, error));
        return response;
    }
}

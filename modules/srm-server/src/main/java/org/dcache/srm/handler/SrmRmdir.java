package org.dcache.srm.handler;

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMNonEmptyDirectoryException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.v2_2.SrmRmdirRequest;
import org.dcache.srm.v2_2.SrmRmdirResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmRmdir
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmRmdir.class);
    private static final String SFN_STRING = "SFN=";

    private final AbstractStorageElement storage;
    private final SrmRmdirRequest request;
    private final SRMUser user;
    private final SRM srm;
    private SrmRmdirResponse response;

    public SrmRmdir(SRMUser user,
                    SrmRmdirRequest request,
                    AbstractStorageElement storage,
                    SRM srm,
                    String clientHost)
    {
        this.srm = srm;
        this.request = checkNotNull(request);
        this.user = checkNotNull(user);
        this.storage = checkNotNull(storage);
    }

    public SrmRmdirResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmRmdir();
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.toString());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMAuthorizationException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_AUTHORIZATION_FAILURE);
            } catch (SRMInvalidPathException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_PATH);
            } catch (SRMNonEmptyDirectoryException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_NON_EMPTY_DIRECTORY);
            } catch (SRMException e) {
                response = getFailedResponse(e.toString());
            }
        }
        return response;
    }

    private SrmRmdirResponse srmRmdir()
            throws SRMException
    {
        URI surl = URI.create(request.getSURL().toString());
        String path = getPath(surl);

        /* If surl is a prefix to any active upload, then we report the directory as
         * non-empty. This is not strictly required by the SRM spec, however S2 tests
         * (usecase.RmdirBeingPutInto) check for this behaviour.
         */
        for (PutFileRequest putFileRequest : srm.getActiveJobs(PutFileRequest.class)) {
            String requestPath = getPath(putFileRequest.getSurl());
            if (path.equals(requestPath)) {
                throw new SRMInvalidPathException("Not a directory");
            }
            if (requestPath.startsWith(path)) {
                throw new SRMNonEmptyDirectoryException("Directory is not empty");
            }
        }

        storage.removeDirectory(user, surl, request.getRecursive() != null && request.getRecursive());
        return new SrmRmdirResponse(new TReturnStatus(TStatusCode.SRM_SUCCESS, null));
    }

    private static String getPath(URI surl)
    {
        String path = surl.getPath();
        String query = surl.getQuery();
        if (query != null) {
            int i = query.indexOf(SFN_STRING);
            if (i != -1) {
                path = query.substring(i + SFN_STRING.length());
            }
        }
        /* REVISIT
         *
         * This is not correct in the presence of symlinked directories. The
         * simplified path may refer to a different directory than the one
         * we will delete.
         *
         * For now we ignore this problem - fixing it requires resolving the
         * paths to an absolute path, which requires additional name space
         * lookups.
         */
        path = Files.simplifyPath(path);
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        return path;
    }

    public static final SrmRmdirResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmRmdirResponse getFailedResponse(String error,
                                                           TStatusCode statusCode)
    {
        SrmRmdirResponse response = new SrmRmdirResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, error));
        return response;
    }
}

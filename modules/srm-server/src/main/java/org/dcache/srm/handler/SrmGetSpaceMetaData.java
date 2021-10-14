package org.dcache.srm.handler;

import static java.util.Objects.requireNonNull;
import static org.dcache.srm.handler.ReturnStatuses.getSummaryReturnStatus;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.v2_2.ArrayOfTMetaDataSpace;
import org.dcache.srm.v2_2.SrmGetSpaceMetaDataRequest;
import org.dcache.srm.v2_2.SrmGetSpaceMetaDataResponse;
import org.dcache.srm.v2_2.TMetaDataSpace;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SrmGetSpaceMetaData {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(SrmGetSpaceMetaData.class);

    private final AbstractStorageElement storage;
    private final SrmGetSpaceMetaDataRequest request;
    private final SRMUser user;
    private SrmGetSpaceMetaDataResponse response;

    public SrmGetSpaceMetaData(SRMUser user,
          SrmGetSpaceMetaDataRequest request,
          AbstractStorageElement storage,
          SRM srm,
          String clientHost) {
        this.request = requireNonNull(request);
        this.user = requireNonNull(user);
        this.storage = requireNonNull(storage);
    }

    public SrmGetSpaceMetaDataResponse getResponse() {
        if (response == null) {
            try {
                response = srmGetSpaceMetaData();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.toString());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            } catch (SRMException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_FAILURE);
            }
        }
        return response;
    }

    private SrmGetSpaceMetaDataResponse srmGetSpaceMetaData()
          throws SRMException {
        String[] spaceTokens = request.getArrayOfSpaceTokens().getStringArray();
        if (spaceTokens == null || spaceTokens.length == 0) {
            throw new SRMInvalidRequestException("arrayOfSpaceToken is empty");
        }
        TMetaDataSpace[] array = storage.srmGetSpaceMetaData(user, spaceTokens);
        return new SrmGetSpaceMetaDataResponse(
              getSummaryReturnStatus(array),
              new ArrayOfTMetaDataSpace(array));
    }

    public static final SrmGetSpaceMetaDataResponse getFailedResponse(String text) {
        return getFailedResponse(text, TStatusCode.SRM_FAILURE);
    }

    public static final SrmGetSpaceMetaDataResponse getFailedResponse(String text,
          TStatusCode statusCode) {
        SrmGetSpaceMetaDataResponse response = new SrmGetSpaceMetaDataResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }
}

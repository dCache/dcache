package org.dcache.srm.handler;

import static java.util.Objects.requireNonNull;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.v2_2.ArrayOfTSupportedTransferProtocol;
import org.dcache.srm.v2_2.SrmGetTransferProtocolsRequest;
import org.dcache.srm.v2_2.SrmGetTransferProtocolsResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TSupportedTransferProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SrmGetTransferProtocols {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(SrmGetTransferProtocols.class);

    private final SRM srm;
    private SrmGetTransferProtocolsResponse response;

    public SrmGetTransferProtocols(SRMUser user,
          SrmGetTransferProtocolsRequest request,
          AbstractStorageElement storage,
          SRM srm,
          String clientHost) {
        this.srm = requireNonNull(srm);
    }

    public SrmGetTransferProtocolsResponse getResponse() {
        if (response == null) {
            try {
                TSupportedTransferProtocol[] protocols = getSupportedTransferProtocols();
                response = new SrmGetTransferProtocolsResponse(
                      new TReturnStatus(TStatusCode.SRM_SUCCESS, null),
                      new ArrayOfTSupportedTransferProtocol(protocols));
            } catch (SRMInternalErrorException e) {
                LOGGER.error(e.getMessage());
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
            }
        }
        return response;
    }

    private TSupportedTransferProtocol[] getSupportedTransferProtocols()
          throws SRMInternalErrorException {
        String[] protocols = srm.getProtocols();
        TSupportedTransferProtocol[] arrayOfProtocols =
              new TSupportedTransferProtocol[protocols.length];
        for (int i = 0; i < protocols.length; ++i) {
            arrayOfProtocols[i] = new TSupportedTransferProtocol(protocols[i], null);
        }
        return arrayOfProtocols;
    }

    public static final SrmGetTransferProtocolsResponse getFailedResponse(String text) {
        return getFailedResponse(text, TStatusCode.SRM_FAILURE);
    }

    public static final SrmGetTransferProtocolsResponse getFailedResponse(String error,
          TStatusCode statusCode) {
        SrmGetTransferProtocolsResponse response = new SrmGetTransferProtocolsResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, error));
        return response;
    }
}

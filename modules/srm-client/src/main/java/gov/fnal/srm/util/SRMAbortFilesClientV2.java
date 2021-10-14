//______________________________________________________________________________
//
// $Id$
// $Author$
//
//
// created 04/08 by Dmitry Litvintsev (litvinse@fnal.gov)
//______________________________________________________________________________

package gov.fnal.srm.util;

import java.io.IOException;
import org.apache.axis.types.URI;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.SrmAbortFilesRequest;
import org.dcache.srm.v2_2.SrmAbortFilesResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;

public class SRMAbortFilesClientV2 extends SRMClient {

    public SRMAbortFilesClientV2(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void start() throws Exception {
        checkCredentialValid();
        StringBuilder sb = new StringBuilder();
        boolean failed = false;
        if (configuration.getArrayOfRequestTokens() != null) {
            for (String requestToken : configuration.getArrayOfRequestTokens()) {
                SrmAbortFilesRequest request = new SrmAbortFilesRequest();
                request.setRequestToken(requestToken);
                ArrayOfAnyURI arrayOfSURLs = new ArrayOfAnyURI();
                URI[] urlArray = new URI[1];
                urlArray[0] = new URI(configuration.getSrmUrl().toASCIIString());
                arrayOfSURLs.setUrlArray(urlArray);
                request.setArrayOfSURLs(arrayOfSURLs);
                SrmAbortFilesResponse response = srm.srmAbortFiles(request);
                if (response == null) {
                    throw new IOException(
                          " null SrmAbortFilesRespinse for request token " + requestToken);
                }
                TReturnStatus rs = response.getReturnStatus();
                if (rs == null) {
                    throw new IOException(" null TReturnStatus for request token " + requestToken);
                }
                if (RequestStatusTool.isFailedRequestStatus(rs)) {
                    failed = true;
                    sb.append("SrmAbortFiles failed for request token ")
                          .append(requestToken).append(":\n ");
                    sb.append("return status: ").append(rs.getStatusCode())
                          .append(", Explanation : ")
                          .append(rs.getExplanation()).append("\n");
                }
                if (response.getArrayOfFileStatuses() != null) {
                    if (response.getArrayOfFileStatuses().getStatusArray() != null) {
                        if (response.getArrayOfFileStatuses().getStatusArray().length > 0) {
                            for (TSURLReturnStatus status : response.getArrayOfFileStatuses()
                                  .getStatusArray()) {
                                TReturnStatus st = status.getStatus();
                                if (st == null) {
                                    sb.append(status.getSurl())
                                          .append(" TReturnStatus is null\n");
                                } else {
                                    sb.append(status.getSurl())
                                          .append(" return code ")
                                          .append(st.getStatusCode())
                                          .append(", Explanation ")
                                          .append(st.getExplanation())
                                          .append("\n");
                                }
                            }
                        } else {
                            sb.append("TSURLReturnStatus is empty\n");
                        }
                    } else {
                        sb.append("TSURLReturnStatus is null\n");
                    }
                } else {
                    sb.append("getArrayOfFileStatuses is null");
                }
            }
            if (failed) {
                throw new IOException(sb.toString());
            }
        } else if (configuration.getSurls() != null) {
            SrmAbortFilesRequest request = new SrmAbortFilesRequest();
            ArrayOfAnyURI arrayOfSURLs = new ArrayOfAnyURI();
            URI[] urlArray = new URI[configuration.getSurls().length];
            for (int i = 0; i < configuration.getSurls().length; i++) {
                urlArray[i] = new URI(
                      (new java.net.URI(configuration.getSurls()[i])).toASCIIString());
            }
            arrayOfSURLs.setUrlArray(urlArray);
            request.setArrayOfSURLs(arrayOfSURLs);
            SrmAbortFilesResponse response = srm.srmAbortFiles(request);
            if (response == null) {
                throw new IOException(" null SrmAbortFilesResponse ");
            }
            TReturnStatus rs = response.getReturnStatus();
            if (rs == null) {
                throw new IOException(" null TReturnStatus ");
            }
            if (RequestStatusTool.isFailedRequestStatus(rs)) {
                sb.append("SrmAbortFiles failed:\n ");
                sb.append("return status: ").append(rs.getStatusCode())
                      .append(", Explanation : ").append(rs.getExplanation())
                      .append("\n");
            }
            if (response.getArrayOfFileStatuses() != null) {
                if (response.getArrayOfFileStatuses().getStatusArray() != null) {
                    if (response.getArrayOfFileStatuses().getStatusArray().length > 0) {
                        for (TSURLReturnStatus status : response.getArrayOfFileStatuses()
                              .getStatusArray()) {
                            TReturnStatus st = status.getStatus();
                            if (st == null) {
                                sb.append(status.getSurl())
                                      .append(" TReturnStatus is null\n");
                            } else {
                                sb.append(status.getSurl())
                                      .append(" return code ")
                                      .append(st.getStatusCode())
                                      .append(", Explanation ")
                                      .append(st.getExplanation())
                                      .append("\n");
                            }
                        }
                    } else {
                        sb.append("TSURLReturnStatus is empty\n");
                    }
                } else {
                    sb.append("TSURLReturnStatus is null\n");
                }
            } else {
                sb.append("getArrayOfFileStatuses is null");
            }
            if (RequestStatusTool.isFailedRequestStatus(rs)) {
                throw new IOException(sb.toString());
            }
        }
    }
}

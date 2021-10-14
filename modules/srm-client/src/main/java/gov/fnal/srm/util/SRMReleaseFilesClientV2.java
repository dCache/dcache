//______________________________________________________________________________
//
// $Id$
// $Author$
//
//
// created 04/08 by Dmitry Litvintsev (litvinse@fnal.gov)
//______________________________________________________________________________

package gov.fnal.srm.util;

import static org.dcache.srm.util.Credentials.checkValid;

import eu.emi.security.authn.x509.X509Credential;
import java.io.IOException;
import org.apache.axis.types.URI;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmReleaseFilesRequest;
import org.dcache.srm.v2_2.SrmReleaseFilesResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;

public class SRMReleaseFilesClientV2 extends SRMClient {

    private ISRM isrm;
    private X509Credential credential;

    public SRMReleaseFilesClientV2(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void connect() throws Exception {
        credential = getCredential();
        isrm = new SRMClientV2(configuration.getSrmUrl(),
              credential,
              configuration.getRetry_timeout(),
              configuration.getRetry_num(),
              doDelegation,
              fullDelegation,
              gss_expected_name,
              configuration.getWebservice_path(),
              configuration.getX509_user_trusted_certificates(),
              configuration.getTransport());
    }

    @Override
    public void start() throws Exception {
        checkValid(credential);
        StringBuilder sb = new StringBuilder();
        boolean failed = false;
        if (configuration.getArrayOfRequestTokens() != null) {
            for (String requestToken : configuration.getArrayOfRequestTokens()) {
                SrmReleaseFilesRequest request = new SrmReleaseFilesRequest();
                request.setRequestToken(requestToken);
                ArrayOfAnyURI arrayOfSURLs = new ArrayOfAnyURI();
                URI[] urlArray = new URI[1];
                urlArray[0] = new URI(configuration.getSrmUrl().toASCIIString());
                arrayOfSURLs.setUrlArray(urlArray);
                request.setArrayOfSURLs(arrayOfSURLs);
                request.setDoRemove(configuration.getDoRemove());
                SrmReleaseFilesResponse response = isrm.srmReleaseFiles(request);
                if (response == null) {
                    throw new IOException(
                          " null SrmReleaseFilesResponse for request token " + requestToken);
                }
                TReturnStatus rs = response.getReturnStatus();
                if (rs == null) {
                    throw new IOException(" null TReturnStatus for request token " + requestToken);
                }
                if (RequestStatusTool.isFailedRequestStatus(rs)) {
                    failed = true;
                    sb.append("SrmReleaseFiles failed for request token ")
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
            SrmReleaseFilesRequest request = new SrmReleaseFilesRequest();
            ArrayOfAnyURI arrayOfSURLs = new ArrayOfAnyURI();
            URI[] urlArray = new URI[configuration.getSurls().length];
            for (int i = 0; i < configuration.getSurls().length; i++) {
                urlArray[i] = new URI(
                      (new java.net.URI(configuration.getSurls()[i])).toASCIIString());
            }
            arrayOfSURLs.setUrlArray(urlArray);
            request.setArrayOfSURLs(arrayOfSURLs);
            SrmReleaseFilesResponse response = isrm.srmReleaseFiles(request);
            if (response == null) {
                throw new IOException(" null SrmReleaseFilesResponse ");
            }
            TReturnStatus rs = response.getReturnStatus();
            if (rs == null) {
                throw new IOException(" null TReturnStatus ");
            }
            if (RequestStatusTool.isFailedRequestStatus(rs)) {
                sb.append("SrmReleaseFiles failed:\n ");
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

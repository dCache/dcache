//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 10/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________


package gov.fnal.srm.util;

import static org.dcache.srm.util.Credentials.checkValid;

import eu.emi.security.authn.x509.X509Credential;
import java.io.IOException;
import java.net.URI;
import java.util.Calendar;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ArrayOfTRequestTokenReturn;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmGetRequestTokensRequest;
import org.dcache.srm.v2_2.SrmGetRequestTokensResponse;
import org.dcache.srm.v2_2.TRequestTokenReturn;
import org.dcache.srm.v2_2.TReturnStatus;

public class SRMGetRequestTokensClientV2 extends SRMClient {

    private URI srmURL;
    private X509Credential credential;
    private ISRM srmv2;

    public SRMGetRequestTokensClientV2(Configuration configuration, URI url) {
        super(configuration);
        srmURL = url;
        try {
            credential = getCredential();
        } catch (Exception e) {
            credential = null;
            System.err.println("Couldn't getGssCredential.");
        }
    }

    @Override
    public void connect() throws Exception {
        srmv2 = new SRMClientV2(srmURL,
              getCredential(),
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
        try {
            SrmGetRequestTokensRequest request = new SrmGetRequestTokensRequest();
            request.setUserRequestDescription(configuration.getUserRequestDescription());
            SrmGetRequestTokensResponse response = srmv2.srmGetRequestTokens(request);

            if (response == null) {
                throw new IOException(" null SrmGetRequestTokensResponse ");
            }
            TReturnStatus rs = response.getReturnStatus();
            if (rs == null) {
                throw new IOException(" null TReturnStatus ");
            }
            if (RequestStatusTool.isFailedRequestStatus(rs)) {
                throw new IOException(
                      "srmGetRequestTokens failed, unexpected or failed return status : " +
                            rs.getStatusCode() + " explanation=" + rs.getExplanation());
            }
            if (response.getArrayOfRequestTokens() != null) {
                ArrayOfTRequestTokenReturn tokens = response.getArrayOfRequestTokens();
                if (tokens.getTokenArray() != null) {
                    TRequestTokenReturn tokenArray[] = tokens.getTokenArray();
                    for (TRequestTokenReturn aTokenArray : tokenArray) {
                        String token = aTokenArray.getRequestToken();
                        Calendar date = aTokenArray.getCreatedAtTime();
                        System.out
                              .println("Request token=" + ((token != null ? token : "null"))
                                    + " Created=" + ((date != null) ? date : "null"));
                    }
                } else {
                    System.err.println("Couldn't get list of request tokens");
                }
            } else {
                System.err.println("No request tokens found");
            }
        } catch (Exception e) {
            say(e.toString());

        }
    }
}

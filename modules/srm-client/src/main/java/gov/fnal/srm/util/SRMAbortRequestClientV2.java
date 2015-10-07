//______________________________________________________________________________
//
// $Id$
// $Author$
//
//
// created 04/08 by Dmitry Litvintsev (litvinse@fnal.gov)
//______________________________________________________________________________

package gov.fnal.srm.util;

import eu.emi.security.authn.x509.X509Credential;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmAbortRequestRequest;
import org.dcache.srm.v2_2.SrmAbortRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;


public class SRMAbortRequestClientV2 extends SRMClient {
    private ISRM isrm;
    private URI srmURL;
    private X509Credential credential;

    public SRMAbortRequestClientV2(Configuration configuration,
                                   URI url) {
        super(configuration);
        srmURL = url;
    }

    @Override
    public void connect() throws Exception {
        credential= getCredential();
        isrm = new SRMClientV2(srmURL,
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
    public void start() throws Exception{
        if (credential.getCertificate().getNotAfter().before(new Date())) {
            throw new RuntimeException("credentials have expired");
        }
        for (String requestToken : configuration.getArrayOfRequestTokens()) {
            try {
                SrmAbortRequestRequest request = new SrmAbortRequestRequest();
                request.setRequestToken(requestToken);
                SrmAbortRequestResponse response =  isrm.srmAbortRequest(request);
                if (response==null) {
                    throw new IOException(" null SrmAbortRequestResponse for request token "+requestToken);
                }
                TReturnStatus rs     = response.getReturnStatus();
                if ( rs == null) {
                    throw new IOException(" null TReturnStatus for request token "+requestToken);
                }
                if (RequestStatusTool.isFailedRequestStatus(rs)) {
                    throw new IOException("SrmAbortRequest failed for "+ requestToken + ",  : "+
                            rs.getStatusCode()+" explanation="+rs.getExplanation());
                }
            }
            catch (Exception e) {
                throw e;
            }
        }
    }
}

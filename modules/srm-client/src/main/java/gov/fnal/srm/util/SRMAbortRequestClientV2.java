//______________________________________________________________________________
//
// $Id$
// $Author$
//
//
// created 04/08 by Dmitry Litvintsev (litvinse@fnal.gov)
//______________________________________________________________________________

package gov.fnal.srm.util;

import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

import java.io.IOException;

import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmAbortRequestRequest;
import org.dcache.srm.v2_2.SrmAbortRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;


public class SRMAbortRequestClientV2 extends SRMClient {
    private ISRM isrm;
    private GlobusURL srmURL;
    private GSSCredential credential;

    public SRMAbortRequestClientV2(Configuration configuration,
                                   GlobusURL url) {
        super(configuration);
        srmURL = url;
    }

    @Override
    public void connect() throws Exception {
        credential=getGssCredential();
        isrm = new SRMClientV2(srmURL,
                credential,
                configuration.getRetry_timeout(),
                configuration.getRetry_num(),
                doDelegation,
                fullDelegation,
                gss_expected_name,
                configuration.getWebservice_path(),
                configuration.getTransport());
    }

    @Override
    public void start() throws Exception{
        try {
            if (credential.getRemainingLifetime() < 60) {
                throw new Exception(
                        "Remaining lifetime of credential is less than a minute.");
            }
        }
        catch (GSSException gsse) {
            throw gsse;
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

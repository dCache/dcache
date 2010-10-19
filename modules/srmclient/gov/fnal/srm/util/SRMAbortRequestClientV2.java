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
import org.globus.util.GlobusURL;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.util.RequestStatusTool;


public class SRMAbortRequestClientV2 extends SRMClient {
    private ISRM isrm;
    private GlobusURL srmURL;
    private org.ietf.jgss.GSSCredential credential = null;

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
                configuration.getWebservice_path());
    }

    @Override
    public void start() throws Exception{
        try {
            if (credential.getRemainingLifetime() < 60)
                throw new Exception(
                "Remaining lifetime of credential is less than a minute.");
        }
        catch (org.ietf.jgss.GSSException gsse) {
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
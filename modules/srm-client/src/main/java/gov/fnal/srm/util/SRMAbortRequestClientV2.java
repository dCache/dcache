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
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.SrmAbortRequestRequest;
import org.dcache.srm.v2_2.SrmAbortRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;

public class SRMAbortRequestClientV2 extends SRMClient {

    public SRMAbortRequestClientV2(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void start() throws Exception {
        checkCredentialValid();
        for (String requestToken : configuration.getArrayOfRequestTokens()) {
            try {
                SrmAbortRequestRequest request = new SrmAbortRequestRequest();
                request.setRequestToken(requestToken);
                SrmAbortRequestResponse response = srm.srmAbortRequest(request);
                if (response == null) {
                    throw new IOException(
                          " null SrmAbortRequestResponse for request token " + requestToken);
                }
                TReturnStatus rs = response.getReturnStatus();
                if (rs == null) {
                    throw new IOException(" null TReturnStatus for request token " + requestToken);
                }
                if (RequestStatusTool.isFailedRequestStatus(rs)) {
                    throw new IOException("SrmAbortRequest failed for " + requestToken + ",  : " +
                          rs.getStatusCode() + " explanation=" + rs.getExplanation());
                }
            } catch (Exception e) {
                throw e;
            }
        }
    }
}

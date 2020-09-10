//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 10/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________


package gov.fnal.srm.util;

import java.io.IOException;
import java.net.URI;
import java.util.Calendar;

import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ArrayOfTRequestTokenReturn;
import org.dcache.srm.v2_2.SrmGetRequestTokensRequest;
import org.dcache.srm.v2_2.SrmGetRequestTokensResponse;
import org.dcache.srm.v2_2.TRequestTokenReturn;
import org.dcache.srm.v2_2.TReturnStatus;

public class SRMGetRequestTokensClientV2 extends SRMClient
{
    private final URI srmURL;

    public SRMGetRequestTokensClientV2(Configuration configuration, URI url) {
        super(configuration);
        srmURL = url;
    }

    @Override
    protected java.net.URI getServerUrl()
    {
        return srmURL;
    }

    @Override
    public void start() throws Exception {
        checkCredentialValid();
        try {
            SrmGetRequestTokensRequest request = new SrmGetRequestTokensRequest();
            request.setUserRequestDescription(configuration.getUserRequestDescription());
            SrmGetRequestTokensResponse response = srm.srmGetRequestTokens(request);

            if ( response == null ) {
                throw new IOException(" null SrmGetRequestTokensResponse ");
            }
            TReturnStatus rs = response.getReturnStatus();
            if ( rs == null) {
                throw new IOException(" null TReturnStatus ");
            }
            if (RequestStatusTool.isFailedRequestStatus(rs)) {
                throw new IOException("srmGetRequestTokens failed, unexpected or failed return status : "+
                        rs.getStatusCode()+" explanation="+rs.getExplanation());
            }
            if (response.getArrayOfRequestTokens()!=null) {
                ArrayOfTRequestTokenReturn tokens = response.getArrayOfRequestTokens();
                if (tokens.getTokenArray()!=null) {
                    TRequestTokenReturn tokenArray[] = tokens.getTokenArray();
                    for (TRequestTokenReturn aTokenArray : tokenArray) {
                        String token = aTokenArray.getRequestToken();
                        Calendar date = aTokenArray.getCreatedAtTime();
                        System.out
                                .println("Request token=" + ((token != null ? token : "null")) + " Created=" + ((date != null) ? date : "null"));
                    }
                }
                else {
                    System.err.println("Couldn't get list of request tokens");
                }
            }
            else {
                System.err.println("No request tokens found");
            }
        }
        catch(Exception e) {
            say(e.toString());

        }
    }
}

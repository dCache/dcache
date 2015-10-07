//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 10/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________


package gov.fnal.srm.util;

import eu.emi.security.authn.x509.X509Credential;

import java.io.IOException;
import java.util.Date;

import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.ArrayOfTRequestSummary;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmGetRequestSummaryRequest;
import org.dcache.srm.v2_2.SrmGetRequestSummaryResponse;
import org.dcache.srm.v2_2.TRequestSummary;
import org.dcache.srm.v2_2.TRequestType;
import org.dcache.srm.v2_2.TReturnStatus;

public class SRMGetRequestSummaryClientV2 extends SRMClient  {

    private java.net.URI srmURL;
    private X509Credential credential;
    private ISRM srmv2;

    public SRMGetRequestSummaryClientV2(Configuration configuration,
                                        java.net.URI url) {
        super(configuration);
        srmURL=url;
        try {
            credential = getCredential();
        }
        catch (Exception e) {
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
        if (credential.getCertificate().getNotAfter().before(new Date())) {
            throw new RuntimeException("credentials have expired");
        }
        try {
            String[] tokens = configuration.getArrayOfRequestTokens();
            SrmGetRequestSummaryRequest request = new SrmGetRequestSummaryRequest();

            request.setArrayOfRequestTokens(new ArrayOfString(tokens));

            SrmGetRequestSummaryResponse response = srmv2.srmGetRequestSummary(request);
            if ( response == null ) {
                throw new IOException(" null SrmGetRequestSummaryResponse ");
            }
            TReturnStatus rs = response.getReturnStatus();
            if ( rs == null) {
                throw new IOException(" null TReturnStatus ");
            }
            if (response.getArrayOfRequestSummaries()!=null) {
                ArrayOfTRequestSummary summaries = response.getArrayOfRequestSummaries();
                if (summaries.getSummaryArray()!=null) {
                    for (int i=0;i<summaries.getSummaryArray().length;i++){
                        TRequestSummary summary = summaries.getSummaryArray(i);
                        if (summary != null) {
                            TReturnStatus st   = summary.getStatus();
                            TRequestType  type = summary.getRequestType();
                            System.out.println("\tRequest number  : "+summary.getRequestToken());
                            System.out.println("\t  Request type  : "+(type!=null ? type.getValue() : "UNKNOWN"));
                            System.out.println("\t Return status");
                            System.out.println("\t\t Status code  : "+(st!=null ? st.getStatusCode() : "null"));
                            System.out.println("\t\t Explanation  : "+(st!=null ? st.getExplanation() : "null"));
                            System.out.println("\tTotal # of files: "+summary.getTotalNumFilesInRequest());
                            System.out.println("\t completed files: "+summary.getNumOfCompletedFiles());
                            System.out.println("\t   waiting files: "+summary.getNumOfWaitingFiles());
                            System.out.println("\t    failed files: "+summary.getNumOfFailedFiles());
                        }
                    }
                }
            }
            if (RequestStatusTool.isFailedRequestStatus(rs)) {
                throw new IOException("srmGetRequestSummary failed, unexpected or failed return status : "+
                        rs.getStatusCode()+" explanation="+rs.getExplanation());
            }
        }
        catch(Exception e) {
            throw e;
        }
    }
}

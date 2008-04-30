// $Id: SRMBringOnlineClientV2.java,v 1.3 2006-12-14 22:46:29 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.2  2006/10/24 22:58:14  litvinse
// added handling of Extra Info parameters in kind of generic
// manner. Now is used to pass request priority to SRM server
//
// Revision 1.1  2006/10/03 18:41:45  timur
// added srmBringOnline and srmPing clients, still untested
//
// Revision 1.18  2006/09/06 15:53:46  timur
//  reformated code, improved error reporting
//
// Revision 1.17  2006/06/21 20:31:56  timur
// Upgraded code to the latest srmv2.2 wsdl (final)" src wsdl sbin/srmv2.2-deploy.wsdd
//
// Revision 1.16  2006/06/21 03:40:27  timur
// updated client to wsdl2.2, need to get latest wsdl next
//
// Revision 1.15  2006/04/21 22:54:29  timur
// better debug info printout
//
// Revision 1.14  2006/03/24 00:29:03  timur
// regenerated stubs with array wrappers for v2_1
//
// Revision 1.13  2006/03/22 01:03:11  timur
// use abort files instead or release in case of interrupts or failures, better CTRL-C handling
//
// Revision 1.12  2006/03/18 00:41:34  timur
// srm v2 bug fixes
//
// Revision 1.11  2006/03/14 18:10:04  timur
// moving toward the axis 1_3
//
// Revision 1.10  2006/02/27 22:54:25  timur
//  do not use Keep Space parameter in srmReleaseFiles, reduce default wait time
//
// Revision 1.9  2006/02/23 22:22:05  neha
// Changes by Neha- For Version 2- allow user specified value of command line option 'webservice_path'
// to override any default value.
//
// Revision 1.8  2006/02/08 23:21:58  neha
// changes by Neha. Added new command line option -storagetype
// Its values could be permanent,volatile or durable;permanent by default
//
// Revision 1.6  2006/01/24 21:14:47  timur
// changes related to the return code
//
// Revision 1.5  2006/01/20 21:50:33  timur
// remove unneeded connect
//
// Revision 1.4  2005/12/14 01:58:44  timur
// srmPrepareToGet client is ready
//
// Revision 1.3  2005/12/09 00:24:51  timur
// srmPrepareToGet works
//
// Revision 1.2  2005/12/08 01:02:07  timur
// working on srmPrepereToGet
//
// Revision 1.1  2005/12/07 02:05:22  timur
// working towards srm v2 get client
//
// Revision 1.21  2005/06/08 22:34:55  timur
// fixed a bug, which led to recognition of some valid file ids as invalid
//
// Revision 1.20  2005/04/27 19:20:55  timur
// make sure client works even if report option is not specified
//                                        String error = "srmPrepareToPut update failed, status : "+ statusCode+
// Revision 1.19  2005/04/27 16:40:00  timur
// more work on report added gridftpcopy and adler32 binaries
//
// Revision 1.18  2005/04/26 02:06:08  timur
// added the ability to create a report file
//
// Revision 1.17  2005/03/11 21:18:36  timur
// making srm compatible with cern tools again
//
// Revision 1.16  2005/01/25 23:20:20  timur
// srmclient now uses srm libraries
//
// Revision 1.15  2005/01/11 18:19:29  timur
// fixed issues related to cern srm, make sure not to change file status for failed files
//
// Revision 1.14  2004/06/30 21:57:04  timur
//  added retries on each step, added the ability to use srmclient used by srm copy in the server, added srm-get-request-status
//

/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.
 
 
 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.
 
 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).
 
 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.
 
 
 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.
 
 
 
  DISCLAIMER OF LIABILITY (BSD):
 
  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.
 
 
  Liabilities of the Government:
 
  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.
 
 
  Export Control:
 
  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

/*
 * SRMBringOnlineClient.java
 *
 * Created on January 28, 2003, 2:54 PM
 */

package gov.fnal.srm.util;

import org.globus.util.GlobusURL;
import diskCacheV111.srm.RequestStatus;
import diskCacheV111.srm.RequestFileStatus;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.util.Iterator;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.util.RequestStatusTool;
/**
 *
 * @author  timur
 */
public class SRMBringOnlineClientV2 extends SRMClient implements Runnable {
    public static String[] protocols;
    GlobusURL from[];
    private HashMap pendingSurlsToIndex = new HashMap();
    private String requestToken;
    private Thread hook;
    private ISRM srmv2;
    /** Creates a new instance of SRMBringOnlineClient */
    public SRMBringOnlineClientV2(Configuration configuration, GlobusURL[] from) {
        super(configuration);
        report = new Report(from,from,configuration.getReport());
        this.protocols = configuration.getProtocols();
        this.from = from;
    }
    
    
    public void connect() throws Exception {
        GlobusURL srmUrl = from[0];
        srmv2 = new SRMClientV2(srmUrl,
                getGssCredential(),
                configuration.getRetry_timeout(),
                configuration.getRetry_num(),
                configuration.getLogger(),
                doDelegation,
                fullDelegation,
                gss_expected_name,
                configuration.getWebservice_path());
    }
    
    public void setProtocols(String[] protocols) {
        this.protocols = protocols;
    }
    
    public void start() throws Exception {
        try {
            int len = from.length;
            String SURLS[] = new String[len];
            TGetFileRequest fileRequests[] = new TGetFileRequest[len];
            String storagetype=configuration.getStorageType();
            for(int i = 0; i < len; ++i) {
                SURLS[i] = from[i].getURL();
                org.apache.axis.types.URI uri =
                        new org.apache.axis.types.URI(SURLS[i]);
                fileRequests[i] = new TGetFileRequest();
                fileRequests[i].setSourceSURL(uri);
                
                //fileRequests[i].setFileStorageType(TFileStorageType.Permanent);
                pendingSurlsToIndex.put(SURLS[i],new Integer(i));
            }
            hook = new Thread(this);
            Runtime.getRuntime().addShutdownHook(hook);
            
            SrmBringOnlineRequest srmBringOnlineRequest = new SrmBringOnlineRequest();
            srmBringOnlineRequest.setDesiredTotalRequestTime(
                    new Integer((int)configuration.getRequestLifetime()));
            //
            srmBringOnlineRequest.setDesiredLifeTime(
                new Integer((int)configuration.getDesiredLifetime()));
            srmBringOnlineRequest.setTargetFileRetentionPolicyInfo(
                    new TRetentionPolicyInfo(TRetentionPolicy.CUSTODIAL,
                    TAccessLatency.NEARLINE));
            
            srmBringOnlineRequest.setArrayOfFileRequests(
                    new ArrayOfTGetFileRequest(fileRequests));
            srmBringOnlineRequest.setTransferParameters(new TTransferParameters(
                    TAccessPattern.TRANSFER_MODE,TConnectionType.WAN,
                    null,new ArrayOfString(protocols)));

	    if (configuration.getExtraParameters().size()>0) { 
		    TExtraInfo[] extraInfoArray = new TExtraInfo[configuration.getExtraParameters().size()];
		    int counter=0;
                    Map extraParameters = configuration.getExtraParameters();
		    for (Iterator i =extraParameters.keySet().iterator(); i.hasNext();) { 
                            String key = (String)i.next();
                            String value = (String)extraParameters.get(key);
			    extraInfoArray[counter++]=new TExtraInfo(key,value);
		    }
		    ArrayOfTExtraInfo arrayOfExtraInfo = new ArrayOfTExtraInfo(extraInfoArray);
		    srmBringOnlineRequest.setStorageSystemInfo(arrayOfExtraInfo);
	    }
            say("calling srmBringOnline");
            SrmBringOnlineResponse response = srmv2.srmBringOnline(srmBringOnlineRequest);
            say("received responce");
            if(response == null) {
                throw new IOException(" null response");
            }
            TReturnStatus status = response.getReturnStatus();
            if(status == null) {
                throw new IOException(" null return status");
            }
            TStatusCode statusCode = status.getStatusCode();
            if(statusCode == null) {
                throw new IOException(" null status code");
            }
            if(RequestStatusTool.isFailedRequestStatus(status)){
                throw new IOException("srmBringOnline submission failed, unexpected or failed status : "+
                        statusCode+" explanation="+status.getExplanation());
            }
            requestToken = response.getRequestToken();
            dsay(" srm returned requestToken = "+requestToken);
            if( response.getArrayOfFileStatuses() == null) {
                throw new IOException("returned bringOnlineRequestFileStatuses is an empty array");
            }
            TBringOnlineRequestFileStatus[] bringOnlineRequestFileStatuses =
                    response.getArrayOfFileStatuses().getStatusArray();
            if(bringOnlineRequestFileStatuses.length != len) {
                throw new IOException("incorrect number of TBringOnlineRequestFileStatus"+
                        "in RequestStatus expected "+len+" received "+
                        bringOnlineRequestFileStatuses.length);
            }
            
            boolean haveCompletedFileRequests = false;
            while(!pendingSurlsToIndex.isEmpty()) {
                long estimatedWaitInSeconds = 5;
                for(int i = 0 ; i<len;++i) {
                    TBringOnlineRequestFileStatus bringOnlineRequestFileStatus = bringOnlineRequestFileStatuses[i];
                    org.apache.axis.types.URI surl = bringOnlineRequestFileStatus.getSourceSURL();
                    if(surl == null) {
                        esay("invalid bringOnlineRequestFileStatus, surl is null");
                        continue;
                    }
                    String surl_string = surl.toString();
                    if(!pendingSurlsToIndex.containsKey(surl_string)) {
                        esay("invalid bringOnlineRequestFileStatus, surl = "+surl_string+" not found");
                        continue;
                    }
                    TReturnStatus fileStatus = bringOnlineRequestFileStatus.getStatus();
                    if(fileStatus == null) {
                        throw new IOException(" null file return status");
                    }
                    TStatusCode fileStatusCode = fileStatus.getStatusCode();
                    if(fileStatusCode == null) {
                        throw new IOException(" null file status code");
                    }
                    if(RequestStatusTool.isFailedFileRequestStatus(fileStatus)){
                        String error ="retreval of surl "+surl_string+" failed, status = "+fileStatusCode+
                                " explanation="+fileStatus.getExplanation();
                        esay(error);
                        int indx = ((Integer) pendingSurlsToIndex.remove(surl_string)).intValue();
                        setReportFailed(from[indx],from[indx],error);
                        haveCompletedFileRequests = true;
                        continue;
                    }
                    if(fileStatus.getStatusCode() == TStatusCode.SRM_SUCCESS ) {
                        int indx = ((Integer) pendingSurlsToIndex.remove(surl_string)).intValue();
                        setReportSucceeded(from[indx],from[indx]);
                        System.out.println(from[indx].getURL()+" brought online, use request id "+requestToken+" to release");
                        haveCompletedFileRequests = true;
                        continue;
                    }
                    if(bringOnlineRequestFileStatus.getEstimatedWaitTime() != null &&
                            bringOnlineRequestFileStatus.getEstimatedWaitTime().intValue() < estimatedWaitInSeconds &&
                            bringOnlineRequestFileStatus.getEstimatedWaitTime().intValue() >=1) {
                        estimatedWaitInSeconds = bringOnlineRequestFileStatus.getEstimatedWaitTime().intValue();
                    }
                }
                
                if(pendingSurlsToIndex.isEmpty()) {
                    dsay("no more pending transfers, breaking the loop");
                    Runtime.getRuntime().removeShutdownHook(hook);
                    break;
                }
                // do not wait longer then 60 seconds
                if(estimatedWaitInSeconds > 60) {
                    estimatedWaitInSeconds = 60;
                }
                try {
                    
                    say("sleeping "+estimatedWaitInSeconds+" seconds ...");
                    Thread.sleep(estimatedWaitInSeconds * 1000);
                } catch(InterruptedException ie) {
                }
                SrmStatusOfBringOnlineRequestRequest srmStatusOfBringOnlineRequestRequest =
                        new SrmStatusOfBringOnlineRequestRequest();
                srmStatusOfBringOnlineRequestRequest.setRequestToken(requestToken);
                // we do not know what to expect from the server when
                // no surls are specified int the status update request
                // so we always are sending the list of all pending srm urls
                //if(haveCompletedFileRequests){
                String [] pendingSurlStrings =
                        (String[])pendingSurlsToIndex.keySet().toArray(new String[0]);
                // if we do not have completed file requests
                // we want to get status for all files
                // we do not need to specify any surls
                int expectedResponseLength= pendingSurlStrings.length;
                org.apache.axis.types.URI surlArray[] = new org.apache.axis.types.URI[expectedResponseLength];
                
                for(int i=0;i<expectedResponseLength;++i){
                    surlArray[i]=new org.apache.axis.types.URI(pendingSurlStrings[i]);;
                }
                
                srmStatusOfBringOnlineRequestRequest.setArrayOfSourceSURLs(
                        new ArrayOfAnyURI(surlArray));
                //}
                //else {
                //    expectedResponseLength = from.length;
                //}
                SrmStatusOfBringOnlineRequestResponse srmStatusOfBringOnlineRequestResponse =
                        srmv2.srmStatusOfBringOnlineRequest(srmStatusOfBringOnlineRequestRequest);
                if(srmStatusOfBringOnlineRequestResponse == null) {
                    throw new IOException(" null srmStatusOfBringOnlineRequestResponse");
                }
                if(srmStatusOfBringOnlineRequestResponse.getArrayOfFileStatuses() == null ) {
                    esay( "incorrect number of RequestFileStatuses");
                    throw new IOException("incorrect number of RequestFileStatuses");
                }
                bringOnlineRequestFileStatuses =
                        srmStatusOfBringOnlineRequestResponse.getArrayOfFileStatuses().getStatusArray();
                
                
                if(bringOnlineRequestFileStatuses == null ||
                        bringOnlineRequestFileStatuses.length !=  expectedResponseLength) {
                    esay( "incorrect number of RequestFileStatuses");
                    throw new IOException("incorrect number of RequestFileStatuses");
                }
                
                status = srmStatusOfBringOnlineRequestResponse.getReturnStatus();
                if(status == null) {
                    throw new IOException(" null return status");
                }
                statusCode = status.getStatusCode();
                if(statusCode == null) {
                    throw new IOException(" null status code");
                }
                if(RequestStatusTool.isFailedRequestStatus(status)){
                    String error = "srmBringOnline update failed, status : "+ statusCode+
                            " explanation="+status.getExplanation();
                    esay(error);
                    for(int i = 0; i<expectedResponseLength;++i) {
                        TReturnStatus frstatus = bringOnlineRequestFileStatuses[i].getStatus();
                        if ( frstatus != null) {
                            esay("BringOnlineFileRequest["+
                                    bringOnlineRequestFileStatuses[i].getSourceSURL()+
                                    "] status="+frstatus.getStatusCode()+
                                    " explanation="+frstatus.getExplanation()
                                    );
                        }
                    }
                    throw new IOException(error);
                }
            }
        } catch(Exception e) {
            say(e.toString());
        } finally {
            report.dumpReport();
            if(!report.everythingAllRight()){
                System.err.println("srm bring online of at least one file failed or not completed");
                System.exit(1);
            }
            
        }
    }
    // this is called when Ctrl-C is hit, or TERM signal received
    public void run() {
        try {
            abortAllPendingFiles();
        }catch(Exception e) {
            logger.elog(e);
        }
    }
    
    private void abortAllPendingFiles() throws Exception{
        if(pendingSurlsToIndex.isEmpty()) {
            return;
        }
        if(requestToken != null) {
            String[] surl_strings = (String[])pendingSurlsToIndex.keySet().toArray(new String[0]);
            int len = surl_strings.length;
            say("Releasing all remaining file requests");
            org.apache.axis.types.URI surlArray[] = new org.apache.axis.types.URI[len];
            
            for(int i=0;i<len;++i){
                surlArray[i]=new org.apache.axis.types.URI(surl_strings[i]);
            }
            SrmAbortFilesRequest srmAbortFilesRequest = new SrmAbortFilesRequest();
            srmAbortFilesRequest.setRequestToken(requestToken);
            srmAbortFilesRequest.setArrayOfSURLs(
                    new ArrayOfAnyURI(surlArray));
            SrmAbortFilesResponse srmAbortFilesResponse = srmv2.srmAbortFiles(srmAbortFilesRequest);
            if(srmAbortFilesResponse == null) {
                logger.elog(" srmAbortFilesResponse is null");
            } else {
                TReturnStatus returnStatus = srmAbortFilesResponse.getReturnStatus();
                if(returnStatus == null) {
                    esay("srmAbortFiles return status is null");
                    return;
                }
                say("srmAbortFiles status code="+returnStatus.getStatusCode());
            }
        }
    }
    
    
}

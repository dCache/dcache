// $Id: SRMPutClientV2.java,v 1.25 2007-09-20 16:59:13 litvinse Exp $
// $Log: not supported by cvs2svn $
// Revision 1.24  2007/03/15 17:43:33  timur
// make overwrite_mode option available
//
// Revision 1.23  2006/12/14 22:45:07  timur
// making code compatible with java 1.4 again
//
// Revision 1.22  2006/11/06 18:42:11  litvinse
// remove logging
//
// Revision 1.21  2006/10/24 22:58:14  litvinse
// added handling of Extra Info parameters in kind of generic
// manner. Now is used to pass request priority to SRM server
//
// Revision 1.20  2006/09/06 15:53:47  timur
//  reformated code, improved error reporting
//
// Revision 1.19  2006/08/21 21:39:22  timur
// better error reporting
//
// Revision 1.18  2006/08/17 02:02:02  timur
// make srmPrepareToPut aware of space token, retention policy and access latency
//
// Revision 1.17  2006/08/10 22:49:35  litvinse
// added explicit space reservation clien
//
// Revision 1.16  2006/06/21 20:31:56  timur
// Upgraded code to the latest srmv2.2 wsdl (final)" src wsdl sbin/srmv2.2-deploy.wsdd
//
// Revision 1.15  2006/06/21 03:40:27  timur
// updated client to wsdl2.2, need to get latest wsdl next
//
// Revision 1.14  2006/04/21 22:54:29  timur
// better debug info printout
//
// Revision 1.13  2006/03/24 00:29:03  timur
// regenerated stubs with array wrappers for v2_1
//
// Revision 1.12  2006/03/22 01:03:11  timur
// use abort files instead or release in case of interrupts or failures, better CTRL-C handling
//
// Revision 1.11  2006/03/18 00:41:34  timur
// srm v2 bug fixes
//
// Revision 1.10  2006/03/14 18:10:04  timur
// moving toward the axis 1_3
//
// Revision 1.9  2006/03/02 19:09:31  neha
// changes by Neha: To fix bug causing NullPointerException
//
// Revision 1.8  2006/02/27 22:54:25  timur
//  do not use Keep Space parameter in srmReleaseFiles, reduce default wait time
//
// Revision 1.7  2006/02/23 22:22:05  neha
// Changes by Neha- For Version 2- allow user specified value of command line option 'webservice_path'
// to override any default value.
//
// Revision 1.6  2006/02/08 23:26:31  neha
// by Neha- removing word 'hahaha' while printing storage type
//
// Revision 1.5  2006/02/08 23:21:58  neha
// changes by Neha. Added new command line option -storagetype
// Its values could be permanent,volatile or durable;permanent by default
//
// Revision 1.3  2006/01/24 21:14:47  timur
// changes related to the return code
//
// Revision 1.2  2006/01/20 21:50:33  timur
// remove unneeded connect
//
// Revision 1.1  2005/12/14 01:58:44  timur
// srmPrepareToPut client is ready
//
// Revision 1.1  2005/12/13 23:07:52  timur
// modifying the names of classes for consistency
//
// Revision 1.24  2005/12/07 02:05:22  timur
// working towards srm v2 get client
//
// Revision 1.23  2005/10/20 21:02:41  timur
// moving SRMCopy to SRMDispatcher
//
// Revision 1.22  2005/09/09 14:31:40  timur
// set sources to the same values as destinations in case of put
//
// Revision 1.21  2005/06/08 22:34:55  timur
// fixed a bug, which led to recognition of some valid file ids as invalid
//
// Revision 1.20  2005/04/27 19:20:55  timur
// make sure client works even if report option is not specified
//
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
// Revision 1.14  2004/06/30 21:57:05  timur
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
 * SRMGetClient.java
 *
 * Created on January 28, 2003, 2:54 PM
 */

package gov.fnal.srm.util;

import org.globus.util.GlobusURL;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.io.IOException;
import java.io.File;
import org.dcache.srm.client.SRMClientV2;
import org.apache.axis.types.URI;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.util.RequestStatusTool;

/**
 *
 * @author  timur
 */
public class SRMPutClientV2 extends SRMClient implements Runnable {
    private GlobusURL from[];
    private GlobusURL to[];
    private String protocols[];
    private HashMap pendingSurlsToIndex = new HashMap();
    private Copier copier;
    private Thread hook;
    private ISRM srmv2;
    private String requestToken;
    private SrmPrepareToPutResponse response;
    /** Creates a new instance of SRMGetClient */
    public SRMPutClientV2(Configuration configuration, GlobusURL[] from, GlobusURL[] to) {
        super(configuration);
        report = new Report(from,to,configuration.getReport());
        this.protocols = configuration.getProtocols();
        this.from = from;
        this.to = to;
    }
    
    
    public void setProtocols(String[] protocols) {
        this.protocols = protocols;
    }
    
    public void connect() throws Exception {
        GlobusURL srmUrl = to[0];
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
    
    public void start() throws Exception {
        try {
            copier = new Copier(urlcopy,configuration);
            copier.setDebug(debug);
            new Thread(copier).start();
            int len = from.length;
            TPutFileRequest fileRequests[] = new TPutFileRequest[len];
            String storagetype=configuration.getStorageType();
            String SURLS[] = new String[len];
            for(int i = 0; i < len; ++i) {
                GlobusURL filesource = from[i];
                int filetype = SRMDispatcher.getUrlType(filesource);
                if((filetype & SRMDispatcher.FILE_URL) == 0) {
                    throw new IOException(" source is not file "+ filesource.getURL());
                }
                if((filetype & SRMDispatcher.DIRECTORY_URL) == SRMDispatcher.DIRECTORY_URL) {
                    throw new IOException(" source is directory "+ filesource.getURL());
                }
                if((filetype & SRMDispatcher.CAN_READ_FILE_URL) == 0) {
                    throw new IOException(" source is not readable "+ filesource.getURL());
                }
                File f = new File(filesource.getPath());
                long filesize = f.length();
                SURLS[i] = to[i].getURL();
                URI uri = new URI(SURLS[i]);
                fileRequests[i] = new TPutFileRequest();
                fileRequests[i].setExpectedFileSize(
                        new org.apache.axis.types.UnsignedLong(filesize));
                fileRequests[i].setTargetSURL(uri);
                pendingSurlsToIndex.put(SURLS[i],new Integer(i));
            }
            
            hook = new Thread(this);
            Runtime.getRuntime().addShutdownHook(hook);
            
            SrmPrepareToPutRequest srmPrepareToPutRequest =
                    new SrmPrepareToPutRequest();
            if(storagetype.equals("volatile")){
                srmPrepareToPutRequest.setDesiredFileStorageType(TFileStorageType.VOLATILE);
            }else if(storagetype.equals("durable")){
                srmPrepareToPutRequest.setDesiredFileStorageType(TFileStorageType.DURABLE);
            }else{
                srmPrepareToPutRequest.setDesiredFileStorageType(TFileStorageType.PERMANENT);
            }
            srmPrepareToPutRequest.setDesiredTotalRequestTime(new Integer((int)configuration.getRequestLifetime()));
            srmPrepareToPutRequest.setTargetFileRetentionPolicyInfo(
                    new TRetentionPolicyInfo(TRetentionPolicy.CUSTODIAL,
                    TAccessLatency.NEARLINE));
            
            srmPrepareToPutRequest.setArrayOfFileRequests(
                    new ArrayOfTPutFileRequest(fileRequests));
            srmPrepareToPutRequest.setTransferParameters(new TTransferParameters(
                    TAccessPattern.TRANSFER_MODE,TConnectionType.WAN,
                    null,new ArrayOfString(protocols)));
            TTransferParameters transferParams = new
                    TTransferParameters();
            if(configuration.getRetentionPolicy() != null &&
                    configuration.getAccessLatency() != null){
                TRetentionPolicy retentionPolicy = TRetentionPolicy.fromString(configuration.getRetentionPolicy());
                TAccessLatency accessLatency = TAccessLatency.fromString(configuration.getAccessLatency());
                TRetentionPolicyInfo retentionPolicyInfo =
                        new TRetentionPolicyInfo(retentionPolicy,accessLatency);
                srmPrepareToPutRequest.setTargetFileRetentionPolicyInfo(retentionPolicyInfo);
                
            }
            if(configuration.getOverwriteMode() != null) {
                srmPrepareToPutRequest.setOverwriteOption(TOverwriteMode.fromString(configuration.getOverwriteMode()));
            }
            if(configuration.getSpaceToken() != null) {
                srmPrepareToPutRequest.setTargetSpaceToken(configuration.getSpaceToken());
            }
            
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
		    srmPrepareToPutRequest.setStorageSystemInfo(arrayOfExtraInfo);
	    }

            //SrmPrepareToPutResponse response = srmv2.srmPrepareToPut(srmPrepareToPutRequest);
            response = srmv2.srmPrepareToPut(srmPrepareToPutRequest);
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
            
            String explanation = status.getExplanation();
            
            if(RequestStatusTool.isFailedRequestStatus(status)){
                if(explanation != null){
                    throw new IOException("srmPrepareToPut submission failed, unexpected or failed status : "+ statusCode+" explanation= "+explanation);
                }else{
                    throw new IOException("srmPrepareToPut submission failed, unexpected or failed status : "+ statusCode);
                }
            }
            
            requestToken = response.getRequestToken();
            dsay(" srm returned requestToken = "+requestToken);
            if(response.getArrayOfFileStatuses() == null  ) {
                throw new IOException("returned PutRequestFileStatuses is an empty array");
            }
            TPutRequestFileStatus[] putRequestFileStatuses =
                    response.getArrayOfFileStatuses().getStatusArray();
            if(putRequestFileStatuses.length != len) {
                throw new IOException("incorrect number of GetRequestFileStatuses"+
                        "in RequestStatus expected "+len+" received "+
                        putRequestFileStatuses.length);
            }
            boolean haveCompletedFileRequests = false;
            while(!pendingSurlsToIndex.isEmpty()) {
                long estimatedWaitInSeconds = 5;
                for(int i = 0 ; i<len;++i) {
                    TPutRequestFileStatus putRequestFileStatus = putRequestFileStatuses[i];
                    URI surl = putRequestFileStatus.getSURL();
                    if(surl == null) {
                        esay("invalid putRequestFileStatus, surl is null");
                        continue;
                    }
                    String surl_string = surl.toString();
                    if(!pendingSurlsToIndex.containsKey(surl_string)) {
                        esay("invalid putRequestFileStatus, surl = "+surl_string+" not found");
                        continue;
                    }
                    TReturnStatus fileStatus = putRequestFileStatus.getStatus();
                    if(fileStatus == null) {
                        throw new IOException(" null file return status");
                    }
                    TStatusCode fileStatusCode = fileStatus.getStatusCode();
                    if(fileStatusCode == null) {
                        throw new IOException(" null file status code");
                    }
                    if(RequestStatusTool.isFailedFileRequestStatus(fileStatus)){
                        String error ="retrieval of surl "+surl_string+" failed, status = "+fileStatusCode+" explanation="+fileStatus.getExplanation();
                        esay(error);
                        int indx = ((Integer) pendingSurlsToIndex.remove(surl_string)).intValue();
                        setReportFailed(from[indx],to[indx],error);
                        haveCompletedFileRequests = true;
                        continue;
                    }
                    if(putRequestFileStatus.getTransferURL() != null &&
                            putRequestFileStatus.getTransferURL() != null) {
                        GlobusURL globusTURL = new GlobusURL(putRequestFileStatus.getTransferURL().toString());
                        int indx = ((Integer) pendingSurlsToIndex.remove(surl_string)).intValue();
                        setReportFailed(from[indx],to[indx],  "received TURL, but did not complete transfer");
                        CopyJob job = new SRMV2CopyJob(from[indx],globusTURL,srmv2,requestToken,logger,to[indx],false,this);
                        copier.addCopyJob(job);
                        haveCompletedFileRequests = true;
                        continue;
                    }
                    if(putRequestFileStatus.getEstimatedWaitTime() != null &&
                            putRequestFileStatus.getEstimatedWaitTime().intValue()< estimatedWaitInSeconds &&
                            putRequestFileStatus.getEstimatedWaitTime().intValue() >=1) {
                        estimatedWaitInSeconds = putRequestFileStatus.getEstimatedWaitTime().intValue();
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
                }catch(InterruptedException ie) {
                    logger.elog(ie);
                }
                SrmStatusOfPutRequestRequest srmStatusOfPutRequestRequest = new SrmStatusOfPutRequestRequest();
                srmStatusOfPutRequestRequest.setRequestToken(requestToken);
                // if we do not have completed file requests
                // we want to get status for all files
                // we do not need to specify any surls
                int expectedResponseLength;
                // we do not know what to expect from the server when
                // no surls are specified int the status update request
                // so we always are sending the list of all pending srm urls
                //if(haveCompletedFileRequests){
                String [] pendingSurlStrings = (String[])pendingSurlsToIndex.keySet().toArray(new String[0]);
                expectedResponseLength= pendingSurlStrings.length;
                URI surlArray[] = new URI[expectedResponseLength];
                for(int i=0;i<expectedResponseLength;++i){
                    surlArray[i]=new org.apache.axis.types.URI(pendingSurlStrings[i]);
                }
                srmStatusOfPutRequestRequest.setArrayOfTargetSURLs(
                        new ArrayOfAnyURI(surlArray));
                //}else {
                //        expectedResponseLength = from.length;
                //}
                SrmStatusOfPutRequestResponse srmStatusOfPutRequestResponse = srmv2.srmStatusOfPutRequest(srmStatusOfPutRequestRequest);
                if(srmStatusOfPutRequestResponse == null) {
                    throw new IOException(" null srmStatusOfPutRequestResponse");
                }
                if(srmStatusOfPutRequestResponse.getArrayOfFileStatuses() == null ) {
                    esay( "putRequestFileStatuses == null");
                    throw new IOException( "putRequestFileStatuses == null");
                }
                putRequestFileStatuses =
                        srmStatusOfPutRequestResponse.getArrayOfFileStatuses().getStatusArray();
                if(  putRequestFileStatuses.length !=  expectedResponseLength) {
                    esay( "incorrect number of RequestFileStatuses");
                    throw new IOException("incorrect number of RequestFileStatuses");
                }
                
                status = srmStatusOfPutRequestResponse.getReturnStatus();
                if(status == null) {
                    throw new IOException(" null return status");
                }
                statusCode = status.getStatusCode();
                if(statusCode == null) {
                    throw new IOException(" null status code");
                }
                if(RequestStatusTool.isFailedRequestStatus(status)){
                    String error = "srmPrepareToPut update failed, status : "+ statusCode+
                            " explanation="+status.getExplanation();
                    esay(error);
                    for(int i = 0; i<expectedResponseLength;++i) {
                        TReturnStatus frstatus = putRequestFileStatuses[i].getStatus();
                        if ( frstatus != null) {
                            esay("PutFileRequest["+
                                    putRequestFileStatuses[i].getSURL()+
                                    "] status="+frstatus.getStatusCode()+
                                    " explanation="+frstatus.getExplanation()
                                    );
                        }
                    }
                    throw new IOException(error);
                }
            }
        }catch(Exception e) {
            esay(e.toString());
            try {
                if(copier != null) {
                    
                    say("stopping copier");
                    copier.stop();
                    abortAllPendingFiles();
                }
            }catch(Exception e1) {
                logger.elog(e1);
            }
        }finally{
            if(copier != null) {
                copier.doneAddingJobs();
                copier.waitCompletion();
            }
            report.dumpReport();
            if(!report.everythingAllRight()){
                System.err.println("srm copy of at least one file failed or not completed");
                System.exit(1);
            }
        }
    }
    
    // this is called when Ctrl-C is hit, or TERM signal received
    public void run() {
        try{
            say("stopping copier");
            copier.stop();
            abortAllPendingFiles();
        }catch(Exception e) {
            logger.elog(e);
        }
    }
    
    private void abortAllPendingFiles() throws Exception{
        if(pendingSurlsToIndex.isEmpty()) {
            return;
        }
        if(response != null) {
            requestToken = response.getRequestToken();
	    if (requestToken!=null) { 
		String[] surl_strings = (String[])pendingSurlsToIndex.keySet().toArray(new String[0]);
		int len = surl_strings.length;
		say("Releasing all remaining file requests");
		URI surlArray[] = new URI[len];
		for(int i=0;i<len;++i){
		    surlArray[i]=new org.apache.axis.types.URI(surl_strings[i]);;
		}
		SrmAbortFilesRequest srmAbortFilesRequest = new SrmAbortFilesRequest();
		srmAbortFilesRequest.setRequestToken(requestToken);
		srmAbortFilesRequest.setArrayOfSURLs(new ArrayOfAnyURI(surlArray));
		SrmAbortFilesResponse srmAbortFilesResponse = srmv2.srmAbortFiles(srmAbortFilesRequest);
		if(srmAbortFilesResponse == null) {
		    logger.elog(" srmAbortFilesResponse is null");
		} 
		else 
		{
		    TReturnStatus returnStatus = srmAbortFilesResponse.getReturnStatus();
		    if(returnStatus == null) {
			esay("srmAbortFiles return status is null");
			return;
		    }
		    say("srmAbortFiles status code="+returnStatus.getStatusCode());
		}
	    }
	    else { 
		if (response.getArrayOfFileStatuses()!=null){ 
		    if (response.getArrayOfFileStatuses().getStatusArray()!=null) { 
			for(int i=0;i<response.getArrayOfFileStatuses().getStatusArray().length;i++) { 
			    org.apache.axis.types.URI surl=response.getArrayOfFileStatuses().getStatusArray(i).getSURL();
			    TReturnStatus fst=response.getArrayOfFileStatuses().getStatusArray(i).getStatus();
			    esay("SURL["+i+"]="+surl.toString()+" status="+fst.getStatusCode()+" explanation="+fst.getExplanation());
			}
		    }
		}
	    }
	}
    }
}

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

import org.apache.axis.types.URI;
import org.apache.axis.types.UnsignedLong;
import org.globus.util.GlobusURL;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.request.AccessLatency;
import org.dcache.srm.request.FileStorageType;
import org.dcache.srm.request.RetentionPolicy;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.ArrayOfTExtraInfo;
import org.dcache.srm.v2_2.ArrayOfTPutFileRequest;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmAbortFilesRequest;
import org.dcache.srm.v2_2.SrmAbortFilesResponse;
import org.dcache.srm.v2_2.SrmPrepareToPutRequest;
import org.dcache.srm.v2_2.SrmPrepareToPutResponse;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TAccessPattern;
import org.dcache.srm.v2_2.TConnectionType;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.v2_2.TOverwriteMode;
import org.dcache.srm.v2_2.TPutFileRequest;
import org.dcache.srm.v2_2.TPutRequestFileStatus;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TTransferParameters;

/**
 *
 * @author  timur
 */
public class SRMPutClientV2 extends SRMClient implements Runnable {
    private GlobusURL from[];
    private GlobusURL to[];
    private String protocols[];
    private HashMap<String,Integer> pendingSurlsToIndex = new HashMap<>();
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

    @Override
    public void connect() throws Exception {
        GlobusURL srmUrl = to[0];
        srmv2 = new SRMClientV2(srmUrl,
                getGssCredential(),
                configuration.getRetry_timeout(),
                configuration.getRetry_num(),
                doDelegation,
                fullDelegation,
                gss_expected_name,
                configuration.getWebservice_path(),
                configuration.getTransport());
    }

    @Override
    public void start() throws Exception {
        try {
            copier = new Copier(urlcopy,configuration);
            copier.setDebug(debug);
            new Thread(copier).start();
            int len = from.length;
            TPutFileRequest fileRequests[] = new TPutFileRequest[len];
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
                        new UnsignedLong(filesize));
                fileRequests[i].setTargetSURL(uri);
                pendingSurlsToIndex.put(SURLS[i], i);
            }

            hook = new Thread(this);
            Runtime.getRuntime().addShutdownHook(hook);

            SrmPrepareToPutRequest srmPrepareToPutRequest =
                new SrmPrepareToPutRequest();
            String storagetype=configuration.getStorageType();
            if (storagetype!=null) {
                srmPrepareToPutRequest.setDesiredFileStorageType(FileStorageType.fromString(storagetype.toUpperCase()).toTFileStorageType());
            }
            srmPrepareToPutRequest.setDesiredTotalRequestTime((int) configuration
                    .getRequestLifetime());

            srmPrepareToPutRequest.setArrayOfFileRequests(
                    new ArrayOfTPutFileRequest(fileRequests));

            TAccessPattern  ap = null;
            if(configuration.getAccessPattern() != null ) {
                ap = TAccessPattern.fromString(configuration.getAccessPattern());
            }
            TConnectionType ct = null;
            if(configuration.getConnectionType() != null ) {
                ct = TConnectionType.fromString(configuration.getConnectionType());
            }
            ArrayOfString protocolArray = null;
            if (protocols != null) {
                protocolArray = new ArrayOfString(protocols);
            }
            ArrayOfString arrayOfClientNetworks = null;
            if (configuration.getArrayOfClientNetworks()!=null) {
                arrayOfClientNetworks = new ArrayOfString(configuration.getArrayOfClientNetworks());
            }
            if (ap!=null || ct!=null || arrayOfClientNetworks !=null || protocolArray != null) {
                srmPrepareToPutRequest.setTransferParameters(new TTransferParameters(ap,
                        ct,
                        arrayOfClientNetworks,
                        protocolArray));
            }
            TRetentionPolicy rp = configuration.getRetentionPolicy() != null ?
                RetentionPolicy.fromString(configuration.getRetentionPolicy()).toTRetentionPolicy() : null;
            TAccessLatency al = configuration.getAccessLatency() != null ?
                AccessLatency.fromString(configuration.getAccessLatency()).toTAccessLatency() : null;
            if ( (al!=null) && (rp==null)) {
                throw new IllegalArgumentException("if access latency is specified, "+
                        "then retention policy have to be specified as well");
            }
            else if ( rp!=null ) {
                srmPrepareToPutRequest.setTargetFileRetentionPolicyInfo(new TRetentionPolicyInfo(rp,al));
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
                Map<String,String> extraParameters = configuration.getExtraParameters();
                for (String key : extraParameters.keySet()) {
                    String value = extraParameters.get(key);
                    extraInfoArray[counter++] = new TExtraInfo(key, value);
                }
                ArrayOfTExtraInfo arrayOfExtraInfo = new ArrayOfTExtraInfo(extraInfoArray);
                srmPrepareToPutRequest.setStorageSystemInfo(arrayOfExtraInfo);
            }
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
                        int indx = pendingSurlsToIndex.remove(surl_string);
                        setReportFailed(from[indx],to[indx],error);
                        continue;
                    }
                    if(putRequestFileStatus.getTransferURL() != null &&
                            putRequestFileStatus.getTransferURL() != null) {
                        GlobusURL globusTURL = new GlobusURL(putRequestFileStatus.getTransferURL().toString());
                        int indx = pendingSurlsToIndex.remove(surl_string);
                        setReportFailed(from[indx],to[indx],  "received TURL, but did not complete transfer");
                        CopyJob job = new SRMV2CopyJob(from[indx],globusTURL,srmv2,requestToken,logger,to[indx],false,this);
                        copier.addCopyJob(job);
                        continue;
                    }
                    if(putRequestFileStatus.getEstimatedWaitTime() != null &&
                            putRequestFileStatus.getEstimatedWaitTime() < estimatedWaitInSeconds &&
                            putRequestFileStatus.getEstimatedWaitTime() >=1) {
                        estimatedWaitInSeconds = putRequestFileStatus
                                .getEstimatedWaitTime();
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
                    logger.elog(ie.toString());
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
                String [] pendingSurlStrings = pendingSurlsToIndex.keySet()
                        .toArray(new String[pendingSurlsToIndex.size()]);
                expectedResponseLength= pendingSurlStrings.length;
                URI surlArray[] = new URI[expectedResponseLength];
                for(int i=0;i<expectedResponseLength;++i){
                    surlArray[i]=new URI(pendingSurlStrings[i]);
                }
                srmStatusOfPutRequestRequest.setArrayOfTargetSURLs(
                        new ArrayOfAnyURI(surlArray));
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
                            if (!RequestStatusTool.isTransientStateStatus(frstatus)) {
                                pendingSurlsToIndex.remove(putRequestFileStatuses[i].getSURL().toString());
                            }
                        }
                    }
                    throw new IOException(error);
                }
            }
        }catch(Exception e) {
            try {
                if(copier != null) {
                    dsay("stopping copier" + e);
                    copier.stop();
                    abortAllPendingFiles();
                }
            }catch(Exception e1) {
                edsay(e1.toString());
            }
            throw e;
        }finally{
            if(copier != null) {
                copier.doneAddingJobs();
                try {
                    copier.waitCompletion();
                }
                catch(Exception e1) {
                    edsay(e1.toString());
                }
            }
            report.dumpReport();
            if(!report.everythingAllRight()){
                System.err.println("srm copy of at least one file failed or not completed");
            }
        }
    }

    // this is called when Ctrl-C is hit, or TERM signal received
    @Override
    public void run() {
        try{
            dsay("stopping copier");
            copier.stop();
            abortAllPendingFiles();
        }catch(Exception e) {
            logger.elog(e.toString());
        }
    }

    private void abortAllPendingFiles() throws Exception{
        if(pendingSurlsToIndex.isEmpty()) {
            return;
        }
        if(response != null) {
            requestToken = response.getRequestToken();
            if (requestToken!=null) {
                String[] surl_strings = pendingSurlsToIndex.keySet()
                        .toArray(new String[pendingSurlsToIndex.size()]);
                int len = surl_strings.length;
                say("Releasing all remaining file requests");
                URI surlArray[] = new URI[len];
                for(int i=0;i<len;++i){
                    surlArray[i]=new URI(surl_strings[i]);
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
                            URI surl=response.getArrayOfFileStatuses().getStatusArray(i).getSURL();
                            TReturnStatus fst=response.getArrayOfFileStatuses().getStatusArray(i).getStatus();
                            esay("SURL["+i+"]="+surl.toString()+" status="+fst.getStatusCode()+" explanation="+fst.getExplanation());
                        }
                    }
                }
            }
        }
    }
}

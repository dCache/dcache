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
 * RemoteTurlGetter.java
 *
 * Created on April 30, 2003, 2:38 PM
 */

package org.dcache.srm.client;

import org.apache.axis.types.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashMap;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.util.SrmUrl;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.ArrayOfTGetFileRequest;
import org.dcache.srm.v2_2.ArrayOfTGetRequestFileStatus;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmPrepareToGetRequest;
import org.dcache.srm.v2_2.SrmPrepareToGetResponse;
import org.dcache.srm.v2_2.SrmReleaseFilesRequest;
import org.dcache.srm.v2_2.SrmReleaseFilesResponse;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse;
import org.dcache.srm.v2_2.TAccessPattern;
import org.dcache.srm.v2_2.TConnectionType;
import org.dcache.srm.v2_2.TGetFileRequest;
import org.dcache.srm.v2_2.TGetRequestFileStatus;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TTransferParameters;
/**
 *
 * @author  timur
 */
public final class RemoteTurlGetterV2 extends TurlGetterPutter {
    private static final Logger logger = LoggerFactory.getLogger(RemoteTurlGetterV2.class);

    private ISRM srmv2;
    protected String SURLs[];
    private HashMap<String,Integer> pendingSurlsToIndex = new HashMap<>();
    protected int number_of_file_reqs;
    protected boolean createdMap;
    private String requestToken;
    private long lifetime;
    SrmPrepareToGetResponse srmPrepareToGetResponse;
    private final Transport transport;

    long retry_timout;
    int retry_num;


    public RemoteTurlGetterV2(AbstractStorageElement storage,
                              RequestCredential credential,String[] SURLs,
                              String[] protocols,PropertyChangeListener listener,
                              long retry_timeout,int retry_num , long lifetime,
                              Transport transport) {
        super(storage,credential,protocols);
        this.SURLs = SURLs;
        this.number_of_file_reqs = SURLs.length;
        this.retry_num = retry_num;
        this.retry_timout = retry_timeout;
        this.lifetime = lifetime;
        this.transport = transport;
        addListener(listener);
    }



    protected  void releaseFile(String surl)  throws RemoteException, URI.MalformedURIException{

        SrmReleaseFilesRequest srmReleaseFilesRequest = new SrmReleaseFilesRequest();
        srmReleaseFilesRequest.setRequestToken(requestToken);
        URI surlArray[] =
            new URI[] { new URI(surl)};
        srmReleaseFilesRequest.setArrayOfSURLs(new ArrayOfAnyURI(surlArray));
        SrmReleaseFilesResponse srmReleaseFilesResponse =
            srmv2.srmReleaseFiles(srmReleaseFilesRequest);
        TReturnStatus returnStatus = srmReleaseFilesResponse.getReturnStatus();
        if(returnStatus == null) {
            logger.error("srmReleaseFiles return status is null");
            return;
        }
        logger.debug("srmReleaseFilesResponse status code="+returnStatus.getStatusCode());

    }

    @Override
    public  void getInitialRequest() throws SRMException
    {
        if(number_of_file_reqs == 0) {
            logger.debug("number_of_file_reqs is 0, nothing to do");
            return;
        }
        logger.debug("SURLs[0] is "+SURLs[0]);
        try {
            SrmUrl srmUrl = new SrmUrl(SURLs[0]);
            srmv2 = new SRMClientV2(srmUrl,
                    credential.getDelegatedCredential(),
                    retry_timout,
                    retry_num,
                    true,
                    true,
                    transport);
            int len = SURLs.length;
            TGetFileRequest fileRequests[] = new TGetFileRequest[len];
            for(int i = 0; i < len; ++i) {
                URI surl = new URI(SURLs[i]);
                fileRequests[i] = new TGetFileRequest();
                fileRequests[i].setSourceSURL(surl);
                pendingSurlsToIndex.put(SURLs[i],i);
            }

            SrmPrepareToGetRequest srmPrepareToGetRequest = new SrmPrepareToGetRequest();
            srmPrepareToGetRequest.setDesiredTotalRequestTime((int)lifetime);
            TTransferParameters transferParameters =
                new TTransferParameters();

            transferParameters.setAccessPattern(TAccessPattern.TRANSFER_MODE);
            transferParameters.setConnectionType(TConnectionType.WAN);
            transferParameters.setArrayOfTransferProtocols(new ArrayOfString(protocols));
            srmPrepareToGetRequest.setTransferParameters(transferParameters);
            // we do not want to do this
            // we do not know which storage type to use and
            // it is read anyway

            ArrayOfTGetFileRequest arrayOfTGetFileRequest =
                new ArrayOfTGetFileRequest ();
            arrayOfTGetFileRequest.setRequestArray(fileRequests);
            srmPrepareToGetRequest.setArrayOfFileRequests(arrayOfTGetFileRequest);
            srmPrepareToGetResponse = srmv2.srmPrepareToGet(srmPrepareToGetRequest);
        }
        catch(Exception e) {
            logger.error("failed to connect to {} {}",SURLs[0],e.getMessage());
            throw new SRMException("failed to connect to "+SURLs[0],e);
        }
    }

    @Override
    public void run() {

        if(number_of_file_reqs == 0) {
            logger.debug("number_of_file_reqs is 0, nothing to do");
            return;
        }
        try {
            int len = SURLs.length;
            if(srmPrepareToGetResponse == null) {
                throw new IOException(" null srmPrepareToGetResponse");
            }
            TReturnStatus status = srmPrepareToGetResponse.getReturnStatus();
            if(status == null) {
                throw new IOException(" null return status");
            }
            TStatusCode statusCode = status.getStatusCode();
            if(statusCode == null) {
                throw new IOException(" null status code");
            }
            if(RequestStatusTool.isFailedRequestStatus(status)){
                throw new IOException("srmPrepareToGet submission failed, unexpected or failed status : "+
                        statusCode+" explanation="+status.getExplanation());
            }
            requestToken = srmPrepareToGetResponse.getRequestToken();
            logger.debug(" srm returned requestToken = "+requestToken);
            ArrayOfTGetRequestFileStatus arrayOfTGetRequestFileStatus  =
                srmPrepareToGetResponse.getArrayOfFileStatuses();
            if(arrayOfTGetRequestFileStatus == null  ) {
                throw new IOException("returned GetRequestFileStatuses is an empty array");
            }
            TGetRequestFileStatus[] getRequestFileStatuses =
                arrayOfTGetRequestFileStatus.getStatusArray();
            if(getRequestFileStatuses == null ) {
                throw new IOException("returned GetRequestFileStatuses is an empty array");
            }
            if(getRequestFileStatuses.length != len) {
                throw new IOException("incorrect number of GetRequestFileStatuses"+
                        "in RequestStatus expected "+len+" received "+
                        getRequestFileStatuses.length);
            }
            boolean haveCompletedFileRequests = false;


            while(!pendingSurlsToIndex.isEmpty()) {
                long estimatedWaitInSeconds = Integer.MAX_VALUE;
                for( TGetRequestFileStatus getRequestFileStatus:getRequestFileStatuses) {
                    URI surl = getRequestFileStatus.getSourceSURL();
                    if(surl == null) {
                        logger.error("invalid getRequestFileStatus, surl is null");
                        continue;
                    }
                    String surl_string = surl.toString();
                    if(!pendingSurlsToIndex.containsKey(surl_string)) {
                        logger.error("invalid getRequestFileStatus, surl = "+surl_string+
                        " not found");
                        continue;
                    }
                    TReturnStatus fileStatus = getRequestFileStatus.getStatus();
                    if(fileStatus == null) {
                        throw new IOException(" null file return status");
                    }
                    TStatusCode fileStatusCode = fileStatus.getStatusCode();
                    if(fileStatusCode == null) {
                        throw new IOException(" null file status code");
                    }
                    if(RequestStatusTool.isFailedFileRequestStatus(fileStatus)){
                        String error ="retreval of surl "+surl_string+
                        " failed, status = "+fileStatusCode+
                        " explanation="+fileStatus.getExplanation();
                        logger.error(error);
                        int indx = pendingSurlsToIndex.remove(surl_string);
                        notifyOfFailure(SURLs[indx], error, requestToken, null);
                        haveCompletedFileRequests = true;
                        continue;
                    }
                    if(getRequestFileStatus.getTransferURL() != null ) {
                        String transferUrl = getRequestFileStatus.getTransferURL().toString();
                        int indx = pendingSurlsToIndex.remove(surl_string);
                        long size=0;
                        if( getRequestFileStatus.getFileSize() != null ) {
                            size = getRequestFileStatus.getFileSize().longValue();
                        }
                        else {
                            logger.error("size is not set in FileStatus for SURL="+SURLs[indx]);
                        }
                        notifyOfTURL(SURLs[indx], transferUrl, requestToken,null,size );
                        haveCompletedFileRequests = true;
                        continue;
                    }
                    if(getRequestFileStatus.getEstimatedWaitTime() != null &&
                            getRequestFileStatus.getEstimatedWaitTime() < estimatedWaitInSeconds &&
                            getRequestFileStatus.getEstimatedWaitTime() >=1) {
                        estimatedWaitInSeconds = getRequestFileStatus
                                .getEstimatedWaitTime();
                    }
                }

                if(pendingSurlsToIndex.isEmpty()) {
                    logger.debug("no more pending transfers, breaking the loop");
                    break;
                }
                // do not wait longer then 60 seconds
                if(estimatedWaitInSeconds > 60) {
                    estimatedWaitInSeconds = 60;
                }
                try {

                    logger.debug("sleeping "+estimatedWaitInSeconds+" seconds ...");
                    Thread.sleep(estimatedWaitInSeconds * 1000);
                }
                catch(InterruptedException ie) {
                }
                SrmStatusOfGetRequestRequest srmStatusOfGetRequestRequest =
                    new SrmStatusOfGetRequestRequest();
                srmStatusOfGetRequestRequest.setRequestToken(requestToken);
                // if we do not have completed file requests
                // we want to get status for all files
                // we do not need to specify any surls
                int expectedResponseLength;
                if(haveCompletedFileRequests){
                    String [] pendingSurlStrings =
                            pendingSurlsToIndex.keySet()
                                    .toArray(new String[pendingSurlsToIndex.size()]);
                    expectedResponseLength= pendingSurlStrings.length;
                    URI surlArray[] = new URI[expectedResponseLength];

                    for(int i=0;i<expectedResponseLength;++i){
                        URI surl =
                            new URI(pendingSurlStrings[i]);
                        surlArray[i]=surl;
                    }

                    srmStatusOfGetRequestRequest.setArrayOfSourceSURLs(new ArrayOfAnyURI(surlArray));
                }
                else {
                    expectedResponseLength = SURLs.length;
                    URI surlArray[] = new URI[expectedResponseLength];

                    for(int i=0;i<expectedResponseLength;++i){
                        URI surl =
                            new URI(SURLs[i]);
                        surlArray[i]=surl;
                    }

                    srmStatusOfGetRequestRequest.setArrayOfSourceSURLs(new ArrayOfAnyURI(surlArray));
                }
                SrmStatusOfGetRequestResponse srmStatusOfGetRequestResponse =
                    srmv2.srmStatusOfGetRequest(srmStatusOfGetRequestRequest);
                if(srmStatusOfGetRequestResponse == null) {
                    throw new IOException(" null srmStatusOfGetRequestResponse");
                }
                arrayOfTGetRequestFileStatus =
                    srmStatusOfGetRequestResponse.getArrayOfFileStatuses();
                if(arrayOfTGetRequestFileStatus == null ) {
                    logger.error( "incorrect number of RequestFileStatuses");
                    throw new IOException("incorrect number of RequestFileStatuses");
                }

                getRequestFileStatuses = arrayOfTGetRequestFileStatus.getStatusArray();

                if(getRequestFileStatuses == null ||
                        getRequestFileStatuses.length !=  expectedResponseLength) {
                    logger.error( "incorrect number of RequestFileStatuses");
                    throw new IOException("incorrect number of RequestFileStatuses");
                }

                status = srmStatusOfGetRequestResponse.getReturnStatus();
                if(status == null) {
                    throw new IOException(" null return status");
                }
                statusCode = status.getStatusCode();
                if(statusCode == null) {
                    throw new IOException(" null status code");
                }
                if(RequestStatusTool.isFailedRequestStatus(status)){
                    throw new IOException("srmPrepareToGet update failed, unexpected or failed status : "+
                            statusCode+" explanation="+status.getExplanation());
                }
            }
        }
        catch(IOException e) {
            logger.error(e.toString());
            notifyOfFailure(e);
        }
    }



    public static void staticReleaseFile(RequestCredential credential,
                                         String surl,
                                         String requestTokenString,
                                         long retry_timeout,
                                         int retry_num,
                                         Transport transport) throws Exception
    {
        SrmUrl srmUrl = new SrmUrl(surl);
        SRMClientV2 srmv2 = new SRMClientV2(srmUrl,
                credential.getDelegatedCredential(),
                retry_timeout,
                retry_num,
                true,
                true,
                transport);
        String requestToken = requestTokenString;
        String[] surl_strings = new String[1];
        surl_strings[0] = surl;
        URI surlArray[] = new URI[1];
        surlArray[0]= new URI(surl);
        SrmReleaseFilesRequest srmReleaseFilesRequest = new SrmReleaseFilesRequest();
        srmReleaseFilesRequest.setRequestToken(requestToken);
        srmReleaseFilesRequest.setArrayOfSURLs(new ArrayOfAnyURI(surlArray));
        SrmReleaseFilesResponse srmReleaseFilesResponse =
            srmv2.srmReleaseFiles(srmReleaseFilesRequest);
        TReturnStatus returnStatus = srmReleaseFilesResponse.getReturnStatus();
        if(returnStatus == null) {
            logger.error("srmReleaseFiles return status is null");
            return;
        }
        logger.debug("srmReleaseFilesResponse status code="+returnStatus.getStatusCode());
    }
}

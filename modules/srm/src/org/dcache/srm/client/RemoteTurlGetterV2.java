// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.13  2006/06/21 20:29:52  timur
// Upgraded code to the latest srmv2.2 wsdl (final)
//
// Revision 1.12  2006/06/20 15:42:16  timur
// initial v2.2 commit, code is based on a week old wsdl, will update the wsdl and code next
//
// Revision 1.11  2006/05/01 20:23:06  timur
// fixed a problems in srmturlGetter/Putter
//
// Revision 1.10  2006/03/24 00:22:16  timur
// regenerated stubs with array wrappers for v2_1
//
// Revision 1.9  2006/03/14 17:44:17  timur
// moving toward the axis 1_3
//
// Revision 1.8  2006/02/24 19:40:16  neha
// changes by Neha- to enable value of command line option 'webservice_path' to override its default value
//
// Revision 1.7  2006/02/03 01:43:38  timur
// make  srm v2 copy work with remote srm v1 and vise versa
//
// Revision 1.6  2006/01/21 01:18:34  timur
// bug fixes in SrmCopy
//
// Revision 1.5  2006/01/19 01:48:21  timur
// more v2 copy work
//
// Revision 1.4  2006/01/12 23:38:10  timur
// first working version of srmCopy
//
// Revision 1.3  2006/01/11 17:29:50  timur
// first unetested implmentation of v2 turl getter and putter
//
// Revision 1.2  2006/01/10 21:20:52  timur
// a bit more progress on built in V2 client
//
// Revision 1.1  2006/01/10 19:03:37  timur
// adding srm v2 built in client
//
// Revision 1.5  2005/08/29 22:52:04  timur
// commiting changes made by Neha needed by OSG
//
// Revision 1.4  2005/03/24 19:16:18  timur
// made built in client always delegate credentials, which is required by LBL's DRM
//
// Revision 1.3  2005/03/13 21:56:28  timur
// more changes to restore compatibility
//
// Revision 1.2  2005/03/11 21:16:25  timur
// making srm compatible with cern tools again
//
// Revision 1.1  2005/01/14 23:07:14  timur
// moving general srm code in a separate repository
//
// Revision 1.3  2005/01/07 20:55:30  timur
// changed the implementation of the built in client to use apache axis soap toolkit
//
// Revision 1.2  2004/08/06 19:35:22  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.7  2004/08/03 16:51:47  timur
// removing unneeded dependancies on dcache
//
// Revision 1.1.2.6  2004/06/30 20:37:23  timur
// added more monitoring functions, added retries to the srm client part, adapted the srmclientv1 for usage in srmcp
//
// Revision 1.1.2.5  2004/06/16 19:44:32  timur
// added cvs logging tags and fermi copyright headers at the top, removed Copier.java and CopyJob.java
//
// Revision 1.1.2.4  2004/06/15 22:15:41  timur
// added cvs logging tags and fermi copyright headers at the top
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
 * RemoteTurlGetter.java
 *
 * Created on April 30, 2003, 2:38 PM
 */

package org.dcache.srm.client;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.util.SrmUrl;
import org.globus.io.urlcopy.UrlCopy;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import org.dcache.srm.SRMException;
import java.beans.PropertyChangeListener;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.Logger;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.util.RequestStatusTool;
/**
 *
 * @author  timur
 */
public class RemoteTurlGetterV2 extends TurlGetterPutter {
    private Object sync = new Object();

    private ISRM srmv2;
    protected String SURLs[];
    private HashMap pendingSurlsToIndex = new HashMap();
    protected int number_of_file_reqs;
    protected boolean createdMap;
    private String requestToken;
    private long lifetime;
    SrmPrepareToGetResponse srmPrepareToGetResponse;

    long retry_timout;
    int retry_num;


    public void say(String s) {
        storage.log("RemoteTurlGetterV2 :"+s);
    }

    public void esay(String s) {
        storage.elog("RemoteTurlGetterV2 :"+s);
    }

    public void esay(Throwable t) {
        storage.elog("RemoteTurlGetterV2 exception");
        storage.elog(t);
    }

    public RemoteTurlGetterV2(AbstractStorageElement storage,
    RequestCredential credential,String[] SURLs,
    String[] protocols,PropertyChangeListener listener,
    long retry_timeout,int retry_num , long lifetime) {
        super(storage,credential,protocols);
        this.SURLs = SURLs;
        this.number_of_file_reqs = SURLs.length;
        this.retry_num = retry_num;
        this.retry_timout = retry_timeout;
        this.lifetime = lifetime;
        addListener(listener);
    }



    protected  void releaseFile(String surl)  throws java.rmi.RemoteException, org.apache.axis.types.URI.MalformedURIException{

        SrmReleaseFilesRequest srmReleaseFilesRequest = new SrmReleaseFilesRequest();
        srmReleaseFilesRequest.setRequestToken(requestToken);
        org.apache.axis.types.URI surlArray[] =
                new org.apache.axis.types.URI[] { new org.apache.axis.types.URI(surl)};
        srmReleaseFilesRequest.setArrayOfSURLs(new org.dcache.srm.v2_2.ArrayOfAnyURI(surlArray));
        SrmReleaseFilesResponse srmReleaseFilesResponse =
        srmv2.srmReleaseFiles(srmReleaseFilesRequest);
        TReturnStatus returnStatus = srmReleaseFilesResponse.getReturnStatus();
        if(returnStatus == null) {
            esay("srmReleaseFiles return status is null");
            return;
        }
        say("srmReleaseFilesResponse status code="+returnStatus.getStatusCode());
        return;

    }

   public  void getInitialRequest() throws SRMException
   {
        if(number_of_file_reqs == 0) {
            say("number_of_file_reqs is 0, nothing to do");
            return;
        }
        say("SURLs[0] is "+SURLs[0]);
        try {
            SrmUrl srmUrl = new SrmUrl(SURLs[0]);
            srmv2 = new SRMClientV2(srmUrl,
            credential.getDelegatedCredential(),
            retry_timout,
            retry_num,
            storage,
            true,
            true);
            int len = SURLs.length;
            TGetFileRequest fileRequests[] = new TGetFileRequest[len];
            for(int i = 0; i < len; ++i) {
                org.apache.axis.types.URI surl = new org.apache.axis.types.URI(SURLs[i]);
                fileRequests[i] = new TGetFileRequest();
                fileRequests[i].setSourceSURL(surl);
                pendingSurlsToIndex.put(SURLs[i],new Integer(i));
            }

            SrmPrepareToGetRequest srmPrepareToGetRequest = new SrmPrepareToGetRequest();
            srmPrepareToGetRequest.setDesiredTotalRequestTime(new Integer((int)lifetime));
            org.dcache.srm.v2_2.TTransferParameters transferParameters =
                new org.dcache.srm.v2_2.TTransferParameters();

            transferParameters.setAccessPattern(org.dcache.srm.v2_2.TAccessPattern.TRANSFER_MODE);
            transferParameters.setConnectionType(org.dcache.srm.v2_2.TConnectionType.WAN);
            transferParameters.setArrayOfTransferProtocols(new org.dcache.srm.v2_2.ArrayOfString(protocols));
            srmPrepareToGetRequest.setTransferParameters(transferParameters);
            // we do not want to do this
            // we do not know which storage type to use and
            // it is read anyway
            //srmPrepareToGetRequest.setDesiredFileStorageType(TFileStorageType.PERMANENT);

            ArrayOfTGetFileRequest arrayOfTGetFileRequest =
                new ArrayOfTGetFileRequest ();
            arrayOfTGetFileRequest.setRequestArray(fileRequests);
            srmPrepareToGetRequest.setArrayOfFileRequests(arrayOfTGetFileRequest);
            srmPrepareToGetResponse = srmv2.srmPrepareToGet(srmPrepareToGetRequest);
       }
        catch(Exception e) {
            throw new SRMException("failed to connect to "+SURLs[0],e);
        }
   }

    public void run() {

        if(number_of_file_reqs == 0) {
            say("number_of_file_reqs is 0, nothing to do");
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
            say(" srm returned requestToken = "+requestToken);
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
                    org.apache.axis.types.URI surl = getRequestFileStatus.getSourceSURL();
                    if(surl == null) {
                        esay("invalid getRequestFileStatus, surl is null");
                        continue;
                    }
                    String surl_string = surl.toString();
                    if(!pendingSurlsToIndex.containsKey(surl_string)) {
                        esay("invalid getRequestFileStatus, surl = "+surl_string+
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
                        esay(error);
                       int indx = ((Integer) pendingSurlsToIndex.remove(surl_string)).
                               intValue();
                        notifyOfFailure(SURLs[indx], error, requestToken, null);
                        haveCompletedFileRequests = true;
                        continue;
                    }
                    if(getRequestFileStatus.getTransferURL() != null ) {
                            String transferUrl = getRequestFileStatus.getTransferURL().toString();
                            int indx = ((Integer) pendingSurlsToIndex.remove(surl_string)).intValue();
                            long size=0;
                            if( getRequestFileStatus.getFileSize() != null ) {
                                size = getRequestFileStatus.getFileSize().longValue();
                            }
                            else {
                                esay("size is not set in FileStatus for SURL="+SURLs[indx]);
                            }
                            notifyOfTURL(SURLs[indx], transferUrl, requestToken,null,new Long(size) );
                            haveCompletedFileRequests = true;
                        continue;
                    }
                    if(getRequestFileStatus.getEstimatedWaitTime() != null &&
                      getRequestFileStatus.getEstimatedWaitTime().intValue()< estimatedWaitInSeconds &&
                       getRequestFileStatus.getEstimatedWaitTime().intValue() >=1) {
                           estimatedWaitInSeconds = getRequestFileStatus.getEstimatedWaitTime().intValue();
                    }
                }

                if(pendingSurlsToIndex.isEmpty()) {
                    say("no more pending transfers, breaking the loop");
                    break;
                }
                // do not wait longer then 60 seconds
                if(estimatedWaitInSeconds > 60) {
                    estimatedWaitInSeconds = 60;
                }
                try {

                    say("sleeping "+estimatedWaitInSeconds+" seconds ...");
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
                        (String[])pendingSurlsToIndex.keySet().toArray(new String[0]);
                    expectedResponseLength= pendingSurlStrings.length;
                    org.apache.axis.types.URI surlArray[] = new org.apache.axis.types.URI[expectedResponseLength];

                    for(int i=0;i<expectedResponseLength;++i){
                        org.apache.axis.types.URI surl =
                            new org.apache.axis.types.URI(pendingSurlStrings[i]);
                        surlArray[i]=surl;
                    }

                    srmStatusOfGetRequestRequest.setArrayOfSourceSURLs(new org.dcache.srm.v2_2.ArrayOfAnyURI(surlArray));
                }
                else {
                    expectedResponseLength = SURLs.length;
                    org.apache.axis.types.URI surlArray[] = new org.apache.axis.types.URI[expectedResponseLength];

                    for(int i=0;i<expectedResponseLength;++i){
                        org.apache.axis.types.URI surl =
                            new org.apache.axis.types.URI(SURLs[i]);
                        surlArray[i]=surl;
                    }

                    srmStatusOfGetRequestRequest.setArrayOfSourceSURLs(new org.dcache.srm.v2_2.ArrayOfAnyURI(surlArray));
                }
                SrmStatusOfGetRequestResponse srmStatusOfGetRequestResponse =
                    srmv2.srmStatusOfGetRequest(srmStatusOfGetRequestRequest);
                if(srmStatusOfGetRequestResponse == null) {
                    throw new IOException(" null srmStatusOfGetRequestResponse");
                }
                arrayOfTGetRequestFileStatus =
                    srmStatusOfGetRequestResponse.getArrayOfFileStatuses();
                if(arrayOfTGetRequestFileStatus == null ) {
                    esay( "incorrect number of RequestFileStatuses");
                    throw new IOException("incorrect number of RequestFileStatuses");
                }

                getRequestFileStatuses = arrayOfTGetRequestFileStatus.getStatusArray();

                if(getRequestFileStatuses == null ||
                    getRequestFileStatuses.length !=  expectedResponseLength) {
                    esay( "incorrect number of RequestFileStatuses");
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
        catch(Exception e) {
            this.esay(e);
            notifyOfFailure(e);
            return;
        }
    }



    public static void staticReleaseFile(RequestCredential credential,
    String surl,
    String requestTokenString,
    long retry_timeout,
    int retry_num,
    Logger logger) throws Exception
    {
        SrmUrl srmUrl = new SrmUrl(surl);
        SRMClientV2 srmv2 = new SRMClientV2(srmUrl,
        credential.getDelegatedCredential(),
        retry_timeout,
        retry_num,
        logger,
        true,
        true);
        String requestToken = requestTokenString;
        String[] surl_strings = new String[1];
        surl_strings[0] = surl;
        org.apache.axis.types.URI surlArray[] = new org.apache.axis.types.URI[1];
        surlArray[0]= new org.apache.axis.types.URI(surl);
        SrmReleaseFilesRequest srmReleaseFilesRequest = new SrmReleaseFilesRequest();
        srmReleaseFilesRequest.setRequestToken(requestToken);
        srmReleaseFilesRequest.setArrayOfSURLs(new org.dcache.srm.v2_2.ArrayOfAnyURI(surlArray));
        //srmReleaseFilesRequest.setKeepSpace(Boolean.FALSE);
        SrmReleaseFilesResponse srmReleaseFilesResponse =
        srmv2.srmReleaseFiles(srmReleaseFilesRequest);
        TReturnStatus returnStatus = srmReleaseFilesResponse.getReturnStatus();
        if(returnStatus == null) {
            logger.elog("srmReleaseFiles return status is null");
            return;
        }
        logger.log("srmReleaseFilesResponse status code="+returnStatus.getStatusCode());
        return;
    }

}

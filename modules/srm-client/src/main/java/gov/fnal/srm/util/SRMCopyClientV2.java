//______________________________________________________________________________
//
// $Id$
// $Author$
//
// Pull mode: copy from remote location to SRM. (e.g. from
// remote to MSS.)              Push mode: copy from SRM
// to remote location.              Always release files from source
// after copy is done.              When removeSourceFiles=true, then
// SRM will  remove the             source files on behalf of the caller
// after copy is done.               In pull mode, send srmRelease()
// to remote location when             transfer is done.
// If in push mode, then after transfer is done, notify
// the caller. User can then release the file. If user             releases
// a file being copied to another location before             it is done,
// then refuse to release.              Note there is no protocol negotiation
// with the client             for this request.              "retryTime"
// means: if all the file transfer for this             request are complete,
// then try previously failed transfers             for a total time
// period of "retryTime".              In case that the retries fail,
// the return should include             an explanation of why the retires
// failed.              When both fromSURL and toSURL are local, perform
// local copy              Empty directories are copied as well.
//
// created 10/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

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
 * SrmCopyClientV2.java
 *
 * Created on January 28, 2003, 2:54 PM
 */

package gov.fnal.srm.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.axis.types.URI;
import org.dcache.srm.request.AccessLatency;
import org.dcache.srm.request.FileStorageType;
import org.dcache.srm.request.OverwriteMode;
import org.dcache.srm.request.RetentionPolicy;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfTCopyFileRequest;
import org.dcache.srm.v2_2.SrmAbortFilesRequest;
import org.dcache.srm.v2_2.SrmAbortFilesResponse;
import org.dcache.srm.v2_2.SrmCopyRequest;
import org.dcache.srm.v2_2.SrmCopyResponse;
import org.dcache.srm.v2_2.SrmStatusOfCopyRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TCopyFileRequest;
import org.dcache.srm.v2_2.TCopyRequestFileStatus;
import org.dcache.srm.v2_2.TDirOption;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

public class SRMCopyClientV2 extends SRMClient implements Runnable {

    private final java.net.URI from[];
    private final java.net.URI to[];
    private final SrmCopyRequest req = new SrmCopyRequest();
    private final HashMap<java.net.URI, Integer> pendingSurlsMap = new HashMap<>();

    private Thread hook;
    private String requestToken;

    public SRMCopyClientV2(Configuration configuration, java.net.URI[] from, java.net.URI[] to) {
        super(configuration);
        report = new Report(from, to, configuration.getReport());
        this.from = from;
        this.to = to;
    }

    @Override
    protected java.net.URI getServerUrl() {
        return configuration.isPushmode() ? from[0] : to[0];
    }

    @Override
    public void start() throws Exception {
        checkCredentialValid();
        try {
            //
            // form the request
            //
            int len = from.length;
            TCopyFileRequest copyFileRequests[] = new TCopyFileRequest[len];

            for (int i = 0; i < from.length; ++i) {
                java.net.URI source = from[i];
                java.net.URI dest = to[i];
                TCopyFileRequest copyFileRequest = new TCopyFileRequest();
                copyFileRequest.setSourceSURL(new URI(source.toASCIIString()));
                copyFileRequest.setTargetSURL(new URI(dest.toASCIIString()));
                TDirOption dirOption = new TDirOption();
                dirOption.setIsSourceADirectory(false);
                dirOption.setAllLevelRecursive(Boolean.TRUE);
                copyFileRequest.setDirOption(dirOption);
                copyFileRequests[i] = copyFileRequest;
                pendingSurlsMap.put(from[i], i);
            }
            hook = new Thread(this);
            Runtime.getRuntime().addShutdownHook(hook);
            String storagetype = configuration.getStorageType();
            if (storagetype != null) {
                req.setTargetFileStorageType(
                      FileStorageType.fromString(storagetype.toUpperCase()).toTFileStorageType());
            }
            req.setUserRequestDescription(configuration.getUserRequestDescription());
            req.setDesiredTotalRequestTime((int) configuration
                  .getRequestLifetime());
            TRetentionPolicy retentionPolicy = configuration.getRetentionPolicy() != null ?
                  RetentionPolicy.fromString(configuration.getRetentionPolicy())
                        .toTRetentionPolicy() : null;
            TAccessLatency accessLatency = configuration.getAccessLatency() != null ?
                  AccessLatency.fromString(configuration.getAccessLatency()).toTAccessLatency()
                  : null;

            if ((accessLatency != null) && (retentionPolicy == null)) {
                throw new IllegalArgumentException("if access latency is specified, " +
                      "then retention policy have to be specified as well");
            } else if (retentionPolicy != null) {
                TRetentionPolicyInfo retentionPolicyInfo =
                      new TRetentionPolicyInfo(retentionPolicy, accessLatency);
                req.setTargetFileRetentionPolicyInfo(retentionPolicyInfo);
            }

            if (configuration.getOverwriteMode() != null) {
                req.setOverwriteOption(OverwriteMode.fromString(configuration.getOverwriteMode())
                      .toTOverwriteMode());
            }
            req.setArrayOfFileRequests(new ArrayOfTCopyFileRequest(copyFileRequests));
            req.setUserRequestDescription("This is User request description");
            if (configuration.getSpaceToken() != null) {
                req.setTargetSpaceToken(configuration.getSpaceToken());
            }
            configuration.getStorageSystemInfo().ifPresent(req::setSourceStorageSystemInfo);
            SrmCopyResponse resp = srm.srmCopy(req);
            if (resp == null) {
                throw new IOException(" null SrmCopyResponse");
            }
            TReturnStatus rs = resp.getReturnStatus();
            requestToken = resp.getRequestToken();
            dsay(" srm returned requestToken = " + requestToken);
            if (rs == null) {
                throw new IOException(" null TReturnStatus ");
            }
            TStatusCode statusCode = rs.getStatusCode();
            if (statusCode == null) {
                throw new IOException(" null status code");
            }
            if (RequestStatusTool.isFailedRequestStatus(rs) &&
                  (statusCode != TStatusCode.SRM_FAILURE
                        || resp.getArrayOfFileStatuses() == null)) {
                String explanation = rs.getExplanation();
                if (explanation != null) {
                    throw new IOException(
                          "srmCopy submission failed, unexpected or failed status : " +
                                statusCode + " explanation= " + explanation);
                } else {
                    throw new IOException(
                          "srmCopy submission failed, unexpected or failed status : " + statusCode);
                }
            }
            if (resp.getArrayOfFileStatuses() == null) {
                throw new IOException(
                      "srmCopy submission failed, arrayOfFileStatuses is null, status code :" +
                            rs.getStatusCode() + " explanation=" + rs.getExplanation());

            }
            TCopyRequestFileStatus[] arrayOfStatuses =
                  resp.getArrayOfFileStatuses().getStatusArray();
            if (arrayOfStatuses.length != len) {
                throw new IOException("number of SrmCopyRequestFileStatuses " +
                      "is SrmRequestStatus is different from exopected " + len + " received " +
                      arrayOfStatuses.length);
            }

            while (!pendingSurlsMap.isEmpty()) {
                long estimatedWaitInSeconds = 5;
                for (TCopyRequestFileStatus copyRequestFileStatus : arrayOfStatuses) {
                    if (copyRequestFileStatus == null) {
                        throw new IOException(" null file status code");
                    }
                    TReturnStatus fileStatus = copyRequestFileStatus
                          .getStatus();
                    if (fileStatus == null) {
                        throw new IOException(" null file return status");
                    }
                    TStatusCode fileStatusCode = fileStatus.getStatusCode();
                    if (fileStatusCode == null) {
                        throw new IOException(" null file status code");
                    }
                    URI from_surl = copyRequestFileStatus.getSourceSURL();
                    URI to_surl = copyRequestFileStatus.getTargetSURL();
                    if (from_surl == null) {
                        throw new IOException("null from_surl");
                    }
                    if (to_surl == null) {
                        throw new IOException("null to_surl");
                    }
                    java.net.URI from_surl_uri = new java.net.URI(from_surl.toString());
                    java.net.URI to_surl_uri = new java.net.URI(to_surl.toString());
                    if (RequestStatusTool
                          .isFailedFileRequestStatus(fileStatus)) {
                        String error = "copy of " + from_surl_uri + " into " + to_surl +
                              " failed, status = " + fileStatusCode +
                              " explanation=" + fileStatus.getExplanation();
                        esay(error);
                        int indx = pendingSurlsMap.remove(from_surl_uri);
                        setReportFailed(from[indx], to[indx], error);

                    } else if (fileStatusCode == TStatusCode.SRM_SUCCESS ||
                          fileStatusCode == TStatusCode.SRM_DONE) {
                        int indx = pendingSurlsMap.remove(from_surl_uri);
                        say(" copying of " + from_surl_uri + " to " + to_surl_uri + " succeeded");
                        setReportSucceeded(from[indx], to[indx]);
                    }
                    if (copyRequestFileStatus.getEstimatedWaitTime() != null &&
                          copyRequestFileStatus
                                .getEstimatedWaitTime() < estimatedWaitInSeconds &&
                          copyRequestFileStatus.getEstimatedWaitTime() >= 1) {
                        estimatedWaitInSeconds = copyRequestFileStatus
                              .getEstimatedWaitTime();
                    }
                }
                if (pendingSurlsMap.isEmpty()) {
                    dsay("no more pending transfers, breaking the loop");
                    Runtime.getRuntime().removeShutdownHook(hook);
                    break;
                }

                if (estimatedWaitInSeconds > 60) {
                    estimatedWaitInSeconds = 60;
                }
                try {

                    say("sleeping " + estimatedWaitInSeconds + " seconds ...");
                    Thread.sleep(estimatedWaitInSeconds * 1000);
                } catch (InterruptedException ie) {
                }
                //
                // check our request
                //
                SrmStatusOfCopyRequestRequest request = new SrmStatusOfCopyRequestRequest();
                request.setRequestToken(requestToken);
                request.setAuthorizationID(req.getAuthorizationID());
                int expectedResponseLength = pendingSurlsMap.size();
                URI surlArrayOfFromSURLs[] = new URI[expectedResponseLength];
                URI surlArrayOfToSURLs[] = new URI[expectedResponseLength];
                Iterator<Integer> it = pendingSurlsMap.values().iterator();
                for (int i = 0; it.hasNext(); i++) {
                    int indx = it.next();
                    TCopyFileRequest copyFileRequest = copyFileRequests[indx];
                    surlArrayOfFromSURLs[i] = copyFileRequest.getSourceSURL();
                    surlArrayOfToSURLs[i] = copyFileRequest.getTargetSURL();
                }
                request.setArrayOfSourceSURLs(
                      new ArrayOfAnyURI(surlArrayOfFromSURLs));
                request.setArrayOfTargetSURLs(
                      new ArrayOfAnyURI(surlArrayOfToSURLs));
                SrmStatusOfCopyRequestResponse copyStatusRequestResponse = srm.srmStatusOfCopyRequest(
                      request);
                if (copyStatusRequestResponse == null) {
                    throw new IOException(" null copyStatusRequestResponse");
                }
                if (copyStatusRequestResponse.getArrayOfFileStatuses() == null) {
                    throw new IOException(
                          "null SrmStatusOfCopyRequestResponse.getArrayOfFileStatuses()");
                }

                arrayOfStatuses =
                      copyStatusRequestResponse.getArrayOfFileStatuses().getStatusArray();

                if (arrayOfStatuses.length != pendingSurlsMap.size()) {
                    esay("incorrect number of arrayOfStatuses " +
                          "in SrmStatusOfCopyRequestResponse expected " +
                          expectedResponseLength + " received " +
                          arrayOfStatuses.length);
                }
                TReturnStatus status = copyStatusRequestResponse.getReturnStatus();
                if (status == null) {
                    throw new IOException(" null return status");
                }
                statusCode = status.getStatusCode();
                if (statusCode == null) {
                    throw new IOException(" null status code");
                }
                if (RequestStatusTool.isFailedRequestStatus(status)) {
                    String error = "srmCopy update failed, status : " + statusCode +
                          " explanation=" + status.getExplanation();
                    esay(error);
                    for (int i = 0; i < expectedResponseLength; ++i) {
                        TReturnStatus frstatus = arrayOfStatuses[i].getStatus();
                        if (frstatus != null) {
                            esay("copyFileRequest[" +
                                  arrayOfStatuses[i].getSourceSURL() +
                                  " , " + arrayOfStatuses[i].getTargetSURL() +
                                  "] status=" + frstatus.getStatusCode() +
                                  " explanation=" + frstatus.getExplanation()
                            );
                            if (!RequestStatusTool.isTransientStateStatus(frstatus)) {
                                pendingSurlsMap.remove(new java.net.URI(
                                      arrayOfStatuses[i].getSourceSURL().toString()));
                            }
                        }
                    }
                    throw new IOException(error);
                }
            }
        } catch (Exception e) {
            try {
                abortAllPendingFiles();
            } catch (Exception e1) {
                edsay(e1.toString());
            }
            throw e;
        } finally {
            report.dumpReport();
            if (!report.everythingAllRight()) {
                report.reportErrors(System.err);
            }
        }
    }

    @Override
    public void run() {
        try {
            dsay("stopping ");
            abortAllPendingFiles();
        } catch (Exception e) {
            logger.elog(e.toString());
        }
    }

    private void abortAllPendingFiles() throws Exception {
        if (pendingSurlsMap.isEmpty()) {
            return;
        }
        if (requestToken == null) {
            return;
        }
        java.net.URI[] surl_strings = pendingSurlsMap.keySet()
              .toArray(new java.net.URI[pendingSurlsMap.size()]);
        int len = surl_strings.length;
        say("Releasing all remaining file requests");
        URI surlArray[] = new URI[len];

        for (int i = 0; i < len; ++i) {
            URI uri =
                  new URI(surl_strings[i].toASCIIString());
            surlArray[i] = uri;
        }
        SrmAbortFilesRequest srmAbortFilesRequest = new SrmAbortFilesRequest();
        srmAbortFilesRequest.setRequestToken(requestToken);
        srmAbortFilesRequest.setArrayOfSURLs(new ArrayOfAnyURI(surlArray));
        SrmAbortFilesResponse srmAbortFilesResponse = srm.srmAbortFiles(srmAbortFilesRequest);
        if (srmAbortFilesResponse == null) {
            logger.elog(" srmAbortFilesResponse is null");
        } else {
            TReturnStatus returnStatus = srmAbortFilesResponse.getReturnStatus();
            if (returnStatus == null) {
                esay("srmAbortFiles return status is null");
                return;
            }
            say("srmAbortFiles status code=" + returnStatus.getStatusCode());
        }
    }
}

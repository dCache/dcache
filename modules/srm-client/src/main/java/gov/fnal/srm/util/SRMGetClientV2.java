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

import java.io.IOException;
import java.util.HashMap;
import org.apache.axis.types.URI;
import org.dcache.srm.request.AccessLatency;
import org.dcache.srm.request.RetentionPolicy;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.ArrayOfTGetFileRequest;
import org.dcache.srm.v2_2.SrmAbortFilesRequest;
import org.dcache.srm.v2_2.SrmAbortFilesResponse;
import org.dcache.srm.v2_2.SrmPrepareToGetRequest;
import org.dcache.srm.v2_2.SrmPrepareToGetResponse;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TAccessPattern;
import org.dcache.srm.v2_2.TConnectionType;
import org.dcache.srm.v2_2.TGetFileRequest;
import org.dcache.srm.v2_2.TGetRequestFileStatus;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TTransferParameters;

/**
 * @author timur
 */
public class SRMGetClientV2 extends SRMClient implements Runnable {

    private final java.net.URI from[];
    private final java.net.URI to[];
    private final HashMap<String, Integer> pendingSurlsToIndex = new HashMap<>();

    private Copier copier;
    private String requestToken;
    private Thread hook;

    /**
     * Creates a new instance of SRMGetClient
     */
    public SRMGetClientV2(Configuration configuration, java.net.URI[] from, java.net.URI[] to) {
        super(configuration);
        report = new Report(from, to, configuration.getReport());
        this.from = from;
        this.to = to;
    }

    @Override
    protected java.net.URI getServerUrl() {
        return from[0];
    }

    @Override
    public void start() throws Exception {
        try {
            copier = new Copier(configuration);
            new Thread(copier).start();
            int len = from.length;
            String SURLS[] = new String[len];
            TGetFileRequest fileRequests[] = new TGetFileRequest[len];
            for (int i = 0; i < len; ++i) {
                SURLS[i] = from[i].toASCIIString();
                URI uri =
                      new URI(SURLS[i]);
                fileRequests[i] = new TGetFileRequest();
                fileRequests[i].setSourceSURL(uri);
                pendingSurlsToIndex.put(SURLS[i], i);
            }
            hook = new Thread(this);
            Runtime.getRuntime().addShutdownHook(hook);
            SrmPrepareToGetRequest srmPrepareToGetRequest = new SrmPrepareToGetRequest();
            srmPrepareToGetRequest.setUserRequestDescription(
                  configuration.getUserRequestDescription());
            srmPrepareToGetRequest.setDesiredTotalRequestTime(
                  (int) configuration.getRequestLifetime());
            TRetentionPolicy rp = configuration.getRetentionPolicy() != null ?
                  RetentionPolicy.fromString(configuration.getRetentionPolicy())
                        .toTRetentionPolicy() : null;
            TAccessLatency al = configuration.getAccessLatency() != null ?
                  AccessLatency.fromString(configuration.getAccessLatency()).toTAccessLatency()
                  : null;
            if ((al != null) && (rp == null)) {
                throw new IllegalArgumentException("if access latency is specified, " +
                      "then retention policy have to be specified as well");
            } else if (rp != null) {
                srmPrepareToGetRequest.setTargetFileRetentionPolicyInfo(
                      new TRetentionPolicyInfo(rp, al));
            }
            srmPrepareToGetRequest.setArrayOfFileRequests(
                  new ArrayOfTGetFileRequest(fileRequests));
            TAccessPattern ap = null;
            if (configuration.getAccessPattern() != null) {
                ap = TAccessPattern.fromString(configuration.getAccessPattern());
            }
            TConnectionType ct = null;
            if (configuration.getConnectionType() != null) {
                ct = TConnectionType.fromString(configuration.getConnectionType());
            }
            ArrayOfString protocolArray = null;
            if (configuration.getProtocols() != null) {
                protocolArray = new ArrayOfString(configuration.getProtocols());
            }
            ArrayOfString arrayOfClientNetworks = null;
            if (configuration.getArrayOfClientNetworks() != null) {
                arrayOfClientNetworks = new ArrayOfString(configuration.getArrayOfClientNetworks());
            }
            if (ap != null || ct != null || arrayOfClientNetworks != null
                  || protocolArray != null) {
                srmPrepareToGetRequest.setTransferParameters(new TTransferParameters(ap,
                      ct,
                      arrayOfClientNetworks,
                      protocolArray));
            }
            configuration.getStorageSystemInfo()
                  .ifPresent(srmPrepareToGetRequest::setStorageSystemInfo);
            say("calling srmPrepareToGet");

            SrmPrepareToGetResponse response = srm.srmPrepareToGet(srmPrepareToGetRequest);
            say("received response");
            if (response == null) {
                throw new IOException(" null response");
            }
            TReturnStatus status = response.getReturnStatus();
            if (status == null) {
                throw new IOException(" null return status");
            }
            TStatusCode statusCode = status.getStatusCode();
            if (statusCode == null) {
                throw new IOException(" null status code");
            }
            if (RequestStatusTool.isFailedRequestStatus(status) &&
                  (statusCode != TStatusCode.SRM_FAILURE
                        || response.getArrayOfFileStatuses() == null)) {
                String explanation = status.getExplanation();
                if (explanation != null) {
                    throw new IOException(
                          "srmPrepareToGet submission failed, unexpected or failed status : " +
                                statusCode + " explanation= " + explanation);
                } else {
                    throw new IOException(
                          "srmPrepareToGet submission failed, unexpected or failed status : "
                                + statusCode);
                }
            }
            requestToken = response.getRequestToken();
            dsay(" srm returned requestToken = " + requestToken);
            if (response.getArrayOfFileStatuses() == null) {
                throw new IOException("returned GetRequestFileStatuses is an empty array");
            }
            TGetRequestFileStatus[] getRequestFileStatuses =
                  response.getArrayOfFileStatuses().getStatusArray();
            if (getRequestFileStatuses.length != len) {
                throw new IOException("incorrect number of GetRequestFileStatuses" +
                      "in RequestStatus expected " + len + " received " +
                      getRequestFileStatuses.length);
            }

            while (!pendingSurlsToIndex.isEmpty()) {
                long estimatedWaitInSeconds = 5;
                for (TGetRequestFileStatus getRequestFileStatus : getRequestFileStatuses) {
                    URI surl = getRequestFileStatus.getSourceSURL();
                    if (surl == null) {
                        esay("invalid getRequestFileStatus, surl is null");
                        continue;
                    }
                    String surl_string = surl.toString();
                    if (!pendingSurlsToIndex.containsKey(surl_string)) {
                        esay("invalid getRequestFileStatus, surl = " + surl_string + " not found");
                        continue;
                    }
                    TReturnStatus fileStatus = getRequestFileStatus.getStatus();
                    if (fileStatus == null) {
                        throw new IOException(" null file return status");
                    }
                    TStatusCode fileStatusCode = fileStatus.getStatusCode();
                    if (fileStatusCode == null) {
                        throw new IOException(" null file status code");
                    }
                    if (RequestStatusTool.isFailedFileRequestStatus(fileStatus)) {
                        String error = "retreval of surl " + surl_string + " failed, status = "
                              + fileStatusCode +
                              " explanation=" + fileStatus.getExplanation();
                        esay(error);
                        int indx = pendingSurlsToIndex.remove(surl_string);
                        setReportFailed(from[indx], to[indx], error);
                        continue;
                    }
                    if (getRequestFileStatus.getTransferURL() != null) {
                        java.net.URI globusTURL = new java.net.URI(
                              getRequestFileStatus.getTransferURL().toString());
                        int indx = pendingSurlsToIndex.remove(surl_string);
                        setReportFailed(from[indx], to[indx],
                              "received TURL, but did not complete transfer");
                        CopyJob job = new SRMV2CopyJob(globusTURL,
                              to[indx], srm, requestToken, logger, from[indx], true, this);
                        copier.addCopyJob(job);
                        continue;
                    }
                    if (getRequestFileStatus.getEstimatedWaitTime() != null &&
                          getRequestFileStatus.getEstimatedWaitTime() < estimatedWaitInSeconds &&
                          getRequestFileStatus.getEstimatedWaitTime() >= 1) {
                        estimatedWaitInSeconds = getRequestFileStatus
                              .getEstimatedWaitTime();
                    }
                }

                if (pendingSurlsToIndex.isEmpty()) {
                    dsay("no more pending transfers, breaking the loop");
                    Runtime.getRuntime().removeShutdownHook(hook);
                    break;
                }
                // do not wait longer then 60 seconds
                if (estimatedWaitInSeconds > 60) {
                    estimatedWaitInSeconds = 60;
                }
                try {

                    say("sleeping " + estimatedWaitInSeconds + " seconds ...");
                    Thread.sleep(estimatedWaitInSeconds * 1000);
                } catch (InterruptedException ie) {
                }
                SrmStatusOfGetRequestRequest srmStatusOfGetRequestRequest =
                      new SrmStatusOfGetRequestRequest();
                srmStatusOfGetRequestRequest.setRequestToken(requestToken);
                // we do not know what to expect from the server when
                // no surls are specified int the status update request
                // so we always are sending the list of all pending srm urls
                String[] pendingSurlStrings =
                      pendingSurlsToIndex.keySet()
                            .toArray(new String[pendingSurlsToIndex.size()]);
                // if we do not have completed file requests
                // we want to get status for all files
                // we do not need to specify any surls
                int expectedResponseLength = pendingSurlStrings.length;
                URI surlArray[] = new URI[expectedResponseLength];

                for (int i = 0; i < expectedResponseLength; ++i) {
                    surlArray[i] = new URI(pendingSurlStrings[i]);
                }

                srmStatusOfGetRequestRequest.setArrayOfSourceSURLs(
                      new ArrayOfAnyURI(surlArray));
                SrmStatusOfGetRequestResponse srmStatusOfGetRequestResponse =
                      srm.srmStatusOfGetRequest(srmStatusOfGetRequestRequest);
                if (srmStatusOfGetRequestResponse == null) {
                    throw new IOException(" null srmStatusOfGetRequestResponse");
                }
                if (srmStatusOfGetRequestResponse.getArrayOfFileStatuses() == null) {
                    String error = "array of RequestFileStatuses is null ";
                    statusCode = status.getStatusCode();
                    if (statusCode != null) {
                        error += " status : " + statusCode +
                              " explanation=" + status.getExplanation();
                    }
                    esay(error);
                    throw new IOException(error);
                }
                getRequestFileStatuses =
                      srmStatusOfGetRequestResponse.getArrayOfFileStatuses().getStatusArray();

                if (getRequestFileStatuses == null ||
                      getRequestFileStatuses.length != expectedResponseLength) {
                    String error = "incorrect number of RequestFileStatuses";
                    statusCode = status.getStatusCode();
                    if (statusCode != null) {
                        error += " status : " + statusCode +
                              " explanation=" + status.getExplanation();
                    }
                    esay(error);
                    throw new IOException(error);
                }

                status = srmStatusOfGetRequestResponse.getReturnStatus();
                if (status == null) {
                    throw new IOException(" null return status");
                }
                statusCode = status.getStatusCode();
                if (statusCode == null) {
                    throw new IOException(" null status code");
                }
                if (RequestStatusTool.isFailedRequestStatus(status)) {
                    String error = "srmPrepareToGet update failed, status : " + statusCode +
                          " explanation=" + status.getExplanation();
                    esay(error);
                    for (int i = 0; i < expectedResponseLength; ++i) {
                        TReturnStatus frstatus = getRequestFileStatuses[i].getStatus();
                        if (frstatus != null) {
                            esay("GetFileRequest[" +
                                  getRequestFileStatuses[i].getSourceSURL() +
                                  "] status=" + frstatus.getStatusCode() +
                                  " explanation=" + frstatus.getExplanation()
                            );
                            if (!RequestStatusTool.isTransientStateStatus(frstatus)) {
                                pendingSurlsToIndex.remove(
                                      getRequestFileStatuses[i].getSourceSURL().toString());
                            }
                        }
                    }
                    throw new IOException(error);
                }
            }
        } catch (Exception e) {
            try {
                if (copier != null) {
                    dsay("stopping copier");
                    copier.stop();
                    abortAllPendingFiles();
                }
            } catch (Exception e1) {
                edsay(e1.toString());
            }
            throw e;
        } finally {
            if (copier != null) {
                copier.doneAddingJobs();
                try {
                    copier.waitCompletion();
                } catch (Exception e1) {
                    edsay(e1.toString());
                }
            }
            report.dumpReport();
            if (!report.everythingAllRight()) {
                report.reportErrors(System.err);
            }
        }
    }

    // this is called when Ctrl-C is hit, or TERM signal received
    @Override
    public void run() {
        try {
            dsay("stopping copier");
            copier.stop();
            abortAllPendingFiles();
        } catch (Exception e) {
            logger.elog(e.toString());
        }
    }

    private void abortAllPendingFiles() throws Exception {
        if (pendingSurlsToIndex.isEmpty()) {
            return;
        }
        if (requestToken != null) {
            String[] surl_strings = pendingSurlsToIndex.keySet()
                  .toArray(new String[pendingSurlsToIndex.size()]);
            int len = surl_strings.length;
            say("Releasing all remaining file requests");
            URI surlArray[] = new URI[len];

            for (int i = 0; i < len; ++i) {
                surlArray[i] = new URI(surl_strings[i]);
            }
            SrmAbortFilesRequest srmAbortFilesRequest = new SrmAbortFilesRequest();
            srmAbortFilesRequest.setRequestToken(requestToken);
            srmAbortFilesRequest.setArrayOfSURLs(
                  new ArrayOfAnyURI(surlArray));
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
}

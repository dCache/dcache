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

package gov.fnal.srm.util;

import com.google.common.base.Strings;
import org.apache.axis.types.URI;

import java.rmi.RemoteException;

import org.dcache.srm.Logger;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmAbortFilesRequest;
import org.dcache.srm.v2_2.SrmAbortFilesResponse;
import org.dcache.srm.v2_2.SrmPutDoneRequest;
import org.dcache.srm.v2_2.SrmPutDoneResponse;
import org.dcache.srm.v2_2.SrmReleaseFilesRequest;
import org.dcache.srm.v2_2.SrmReleaseFilesResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SRMV2CopyJob implements CopyJob {
    private java.net.URI from;
    private java.net.URI to;
    private ISRM srm;
    private boolean isDone;
    private Logger logger;

    // added these to support generation of the report
    private java.net.URI surl;
    private boolean isSrmPrepareToGet;
    private SRMClient client;
    private String requestToken;


    public SRMV2CopyJob(java.net.URI from, java.net.URI to, ISRM srm, String requestToken, Logger logger, java.net.URI surl, boolean isSrmPrepareToGet, SRMClient client) {
        this.from = checkNotNull(from);
        this.to = checkNotNull(to);
        this.srm = checkNotNull(srm);
        this.requestToken = checkNotNull(requestToken);
        this.logger = checkNotNull(logger);
        this.surl = checkNotNull(surl);
        this.isSrmPrepareToGet = isSrmPrepareToGet;
        this.client = checkNotNull(client);
    }

    @Override
    public java.net.URI getSource() {
        return from;
    }

    @Override
    public java.net.URI getDestination() {
        return to;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof SRMV2CopyJob)) {
            return false;
        }

        SRMV2CopyJob copy_job = (SRMV2CopyJob) o;

        return to.equals(copy_job.to) && from.equals(copy_job.from);

    }

    @Override
    public String toString() {
        return "CopyJob, source = "+from+" destination = "+to;
    }

    @Override
    public void done(boolean success, String error) {
        synchronized (this) {
            if (isDone) {
                return;
            }
            isDone = true;
        }

        try {
            URI surlArray[] = new URI[]{ new URI(surl.toASCIIString()) };
            if (!success) {
                SrmAbortFilesRequest srmAbortFilesRequest = new SrmAbortFilesRequest();
                srmAbortFilesRequest.setRequestToken(requestToken);
                srmAbortFilesRequest.setArrayOfSURLs(new ArrayOfAnyURI(surlArray));
                SrmAbortFilesResponse srmAbortFilesResponse = srm.srmAbortFiles(srmAbortFilesRequest);
                if (srmAbortFilesResponse == null) {
                    logger.elog("srmAbortFilesResponse is null");
                } else {
                    TReturnStatus returnStatus = srmAbortFilesResponse.getReturnStatus();
                    if (returnStatus == null) {
                        logger.elog("srmAbortFiles return status is null");
                    } else {
                        logger.log("srmAbortFiles status code=" + returnStatus.getStatusCode());
                    }
                }
            } else if (isSrmPrepareToGet) {
                SrmReleaseFilesRequest srmReleaseFilesRequest = new SrmReleaseFilesRequest();
                srmReleaseFilesRequest.setRequestToken(requestToken);
                srmReleaseFilesRequest.setArrayOfSURLs(new ArrayOfAnyURI(surlArray));
                SrmReleaseFilesResponse srmReleaseFilesResponse =
                        srm.srmReleaseFiles(srmReleaseFilesRequest);
                TReturnStatus returnStatus = srmReleaseFilesResponse.getReturnStatus();
                if (returnStatus == null) {
                    success = false;
                    error = "srmReleaseFilesResponse return status is null";
                } else {
                    logger.log("srmReleaseFilesResponse status code=" + returnStatus.getStatusCode());
                }
            } else {
                SrmPutDoneRequest srmPutDoneRequest = new SrmPutDoneRequest();
                srmPutDoneRequest.setRequestToken(requestToken);
                srmPutDoneRequest.setArrayOfSURLs(new ArrayOfAnyURI(surlArray));
                SrmPutDoneResponse srmPutDoneResponse =srm.srmPutDone(srmPutDoneRequest);
                TReturnStatus returnStatus = srmPutDoneResponse.getReturnStatus();
                if (returnStatus == null) {
                    success = false;
                    error = "srmPutDone return status is null";
                } else if (returnStatus.getStatusCode() != TStatusCode.SRM_SUCCESS) {
                    success = false;
                    ArrayOfTSURLReturnStatus arrayOfFileStatuses = srmPutDoneResponse.getArrayOfFileStatuses();
                    if (arrayOfFileStatuses != null &&
                            arrayOfFileStatuses.getStatusArray() != null &&
                            arrayOfFileStatuses.getStatusArray().length > 0 &&
                            arrayOfFileStatuses.getStatusArray()[0].getStatus().getStatusCode() != TStatusCode.SRM_SUCCESS &&
                            !Strings.isNullOrEmpty(arrayOfFileStatuses.getStatusArray()[0].getStatus().getExplanation())) {
                        error = arrayOfFileStatuses.getStatusArray()[0].getStatus().getExplanation();
                    } else {
                        error = returnStatus.getExplanation();
                    }
                }
            }
        } catch (URI.MalformedURIException | RemoteException e) {
            if (success) {
                success = false;
                error = e.toString();
            } else {
                logger.elog(e.toString());
            }
        } finally {
            if (success) {
                if (isSrmPrepareToGet) {
                    client.setReportSucceeded(surl, null);
                } else {
                    client.setReportSucceeded(null, surl);
                }
            } else {
                error = "received TURL but failed to copy: " + error;
                if (isSrmPrepareToGet) {
                    client.setReportFailed(surl, null, error);
                } else {
                    client.setReportFailed(null, surl, error);
                }
            }
        }
    }
}

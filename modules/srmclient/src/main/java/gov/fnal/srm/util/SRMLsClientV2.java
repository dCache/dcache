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
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

import java.io.IOException;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmAbortRequestRequest;
import org.dcache.srm.v2_2.SrmAbortRequestResponse;
import org.dcache.srm.v2_2.SrmLsRequest;
import org.dcache.srm.v2_2.SrmLsResponse;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TFileLocality;
import org.dcache.srm.v2_2.TFileStorageType;
import org.dcache.srm.v2_2.TFileType;
import org.dcache.srm.v2_2.TGroupPermission;
import org.dcache.srm.v2_2.TMetaDataPathDetail;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TUserPermission;


public class SRMLsClientV2 extends SRMClient implements Runnable {
    private GSSCredential cred;
    private GlobusURL surls[];
    private String surl_strings[];
    private ISRM isrm;
    private String requestToken;
    private Thread hook;

    public SRMLsClientV2(Configuration configuration,
                         GlobusURL[] surls,
                         String[] surl_strings) {
        super(configuration);
        this.surls = surls;
        this.surl_strings=surl_strings;
        try {
            cred = getGssCredential();
        }
        catch (Exception e) {
            cred = null;
            System.err.println("Couldn't getGssCredential.");
        }
    }

    @Override
    public void connect() throws Exception {
        GlobusURL srmUrl = surls[0];
        isrm = new SRMClientV2(srmUrl,
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
            if(cred.getRemainingLifetime() < 60) {
                throw new Exception("Remaining lifetime of credential is less than a minute.");
            }
        }
        catch(GSSException gsse) {
            throw gsse;
        }
        try {
            SrmLsRequest req = new SrmLsRequest();
            req.setAllLevelRecursive(Boolean.FALSE);
            req.setFullDetailedList(configuration.isLongLsFormat());
            req.setNumOfLevels(configuration.getRecursionDepth());
            req.setOffset(configuration.getLsOffset());
            if (configuration.getLsCount()!=null) {
                req.setCount(configuration.getLsCount());
            }
            URI[] turlia = new URI[surls.length];
            for(int i =0; i<surls.length; ++i) {
                turlia[i] = new URI(surl_strings[i]);
            }
            req.setArrayOfSURLs(new ArrayOfAnyURI(turlia));
            hook = new Thread(this);
            Runtime.getRuntime().addShutdownHook(hook);
            SrmLsResponse response = isrm.srmLs(req);
            if(response == null){
                throw new Exception ("srm ls response is null!");
            }
            TReturnStatus rs     = response.getReturnStatus();
            requestToken         = response.getRequestToken();
            if ( rs == null) {
                throw new IOException(" null TReturnStatus ");
            }
            StringBuffer sb = new StringBuffer();
            String statusText="Return status:\n" +
            " - Status code:  " +
            response.getReturnStatus().getStatusCode().getValue() + '\n' +
            " - Explanation:  " + response.getReturnStatus().getExplanation();
            logger.log(statusText);
            if (RequestStatusTool.isFailedRequestStatus(rs)) {
                sb.append(statusText).append('\n');
            }
            if (!RequestStatusTool.isTransientStateStatus(rs)) {
                if(response.getDetails() == null){
                    throw new IOException(sb.toString()+"srm ls response path details array is null!");
                }
                else {
                    if (response.getDetails().getPathDetailArray()!=null) {
                        TMetaDataPathDetail[] details = response.getDetails().getPathDetailArray();
                        printResults(sb,details,0," ",configuration.isLongLsFormat());
                    }
                }
                if (RequestStatusTool.isFailedRequestStatus(rs)){
                    throw new IOException(sb.toString());
                }
                System.out.println(sb.toString());
            }
            else {
                if (requestToken==null) {
                    throw new IOException("Request is queued on the server, however the server did not provide a request token.");
                }
                if (RequestStatusTool.isFailedRequestStatus(rs)){
                    throw new IOException(sb.toString());
                }
                // we assume this is asynchronous call
                SrmStatusOfLsRequestRequest statusRequest = new SrmStatusOfLsRequestRequest();
                statusRequest.setRequestToken(requestToken);
                statusRequest.setOffset(req.getOffset());
                statusRequest.setCount(req.getCount());
                statusRequest.setAuthorizationID(req.getAuthorizationID());
                long estimatedWaitInSeconds = 2;
                while(true) {
                    if(estimatedWaitInSeconds > 60) {
                        estimatedWaitInSeconds = 60;
                    }
                    try {
                        say("sleeping "+estimatedWaitInSeconds+" seconds ...");
                        Thread.sleep(estimatedWaitInSeconds * 1000);
                    }
                    catch(InterruptedException ie) {
                        esay("Interrupted, quitting");
                        if ( requestToken != null  ) {
                            abortRequest();
                        }
                        System.exit(1);
                    }
                    estimatedWaitInSeconds*=2;
                    SrmStatusOfLsRequestResponse statusResponse = isrm.srmStatusOfLsRequest(statusRequest);
                    if (statusResponse==null) {
                        throw new IOException("SrmStatusOfLsRequestResponse is null for request "+requestToken);
                    }
                    TReturnStatus status = statusResponse.getReturnStatus();
                    if ( status == null ) {
                        throw new IOException(" null return status");
                    }
                    if ( status.getStatusCode() == null ) {
                        throw new IOException(" null status code");
                    }
                    if (!RequestStatusTool.isTransientStateStatus(status)) {
                        statusText="Return status:\n" +
                        " - Status code:  " +
                        status.getStatusCode().getValue() + '\n' +
                        " - Explanation:  " + status.getExplanation() + '\n' +
                        " - request token: " +requestToken;
                        logger.log(statusText);
                        if (RequestStatusTool.isFailedRequestStatus(status)){
                            sb.append(statusText).append('\n');
                        }
                        if(statusResponse.getDetails() == null){
                            throw new IOException(sb.toString()+"srm ls response path details array is null!");
                        }
                        else {
                            if (statusResponse.getDetails().getPathDetailArray()!=null) {
                                TMetaDataPathDetail[] details = statusResponse.getDetails().getPathDetailArray();
                                printResults(sb,details,0," ",configuration.isLongLsFormat());
                                if (RequestStatusTool.isFailedRequestStatus(status)){
                                    throw new IOException(sb.toString());
                                }
                                System.out.println(sb.toString());
                            }
                        }
                        break;
                    }
                }
            }
        }
        catch (Exception e) {
            esay(e.getMessage());
            try {
                if ( requestToken != null ) {
                    abortRequest();
                }
            }
            catch (Exception e1) {
                logger.elog(e1.toString());
            }
            finally {
                Runtime.getRuntime().removeShutdownHook(hook);
                System.exit(1);
            }
        }
        finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    @Override
    public void run() {
        try {
            say("stopping ");
            if ( requestToken != null ) {
                abortRequest();
            }
        }
        catch(Exception e) {
            logger.elog(e.toString());
        }
    }

    public void abortRequest() throws Exception {
        SrmAbortRequestRequest abortRequest = new SrmAbortRequestRequest();
        abortRequest.setRequestToken(requestToken);
        SrmAbortRequestResponse abortResponse = isrm.srmAbortRequest(abortRequest);
        if (abortResponse == null) {
            logger.elog(" SrmAbort is null");

        }
        else {
            TReturnStatus returnStatus = abortResponse.getReturnStatus();
            if(returnStatus == null) {
                esay("srmAbort return status is null");
                return;
            }
            say("srmAbortRequest status code="+returnStatus.getStatusCode());
        }
    }

    public URI getTSURLInfo(String surl) throws Exception {
        URI uri = new URI(surl);
        return uri;
    }


    public static void printResults(StringBuffer sb,
                                    TMetaDataPathDetail[] ta,
                                    int depth,
                                    String depthPrefix,
                                    boolean longFormat) {
        if  (ta != null) {
            for (int i = 0; i < ta.length; i++) {
                TMetaDataPathDetail metaDataPathDetail = ta[i];
                if(metaDataPathDetail != null){
                    if (metaDataPathDetail.getStatus().getStatusCode() ==
                        TStatusCode.fromString(TStatusCode._SRM_INVALID_PATH)) {
                        sb.append(TStatusCode._SRM_INVALID_PATH).append(" ")
                                .append(depthPrefix).append(" File/directory ")
                                .append(i).append(" ")
                                .append(metaDataPathDetail.getPath())
                                .append(" does not exist. \n");
                    }
                    else {
                        sb.append(depthPrefix);
                        UnsignedLong size =metaDataPathDetail.getSize();
                        if(size != null) {
                            sb.append(" ").append( size.longValue());
                        }
                        sb.append(" ").append( metaDataPathDetail.getPath());
                        if (metaDataPathDetail.getType()==TFileType.DIRECTORY) {
                            sb.append("/");
                        }
                        if (metaDataPathDetail.getStatus().getStatusCode()!=TStatusCode.SRM_SUCCESS){
                            sb.append(" (")
                                    .append(metaDataPathDetail.getStatus()
                                            .getStatusCode()).append(",")
                                    .append(metaDataPathDetail.getStatus()
                                            .getExplanation()).append(")");
                        }
                        sb.append('\n');
                        if(longFormat) {
                            sb.append(" space token(s) :");
                            if (metaDataPathDetail.getArrayOfSpaceTokens()!=null) {
                                for (int j=0;j<metaDataPathDetail.getArrayOfSpaceTokens().getStringArray().length;j++) {
                                    if (j==metaDataPathDetail.getArrayOfSpaceTokens().getStringArray().length-1) {
                                        sb.append(metaDataPathDetail.getArrayOfSpaceTokens().getStringArray()[j]);
                                    }
                                    else {
                                        sb.append(metaDataPathDetail
                                                .getArrayOfSpaceTokens()
                                                .getStringArray()[j])
                                                .append(",");
                                    }
                                }
                            }
                            else {
                                sb.append("none found");
                            }
                            sb.append('\n');
                            TFileStorageType stortype= metaDataPathDetail.getFileStorageType();
                            if(stortype != null) {
                                sb.append(depthPrefix);
                                sb.append(" storage type:").append(stortype.getValue());
                                sb.append('\n');
                            }
                            else {
                                sb.append(" type: null");
                                sb.append('\n');
                            }
                            TRetentionPolicyInfo rpi = metaDataPathDetail.getRetentionPolicyInfo();
                            if (rpi != null) {
                                TRetentionPolicy rt = rpi.getRetentionPolicy();
                                if (rt != null) {
                                    sb.append(depthPrefix);
                                    sb.append(" retention policy:").append(rt.getValue());
                                    sb.append('\n');
                                }
                                else {
                                    sb.append(" retention policy: null");
                                    sb.append('\n');
                                }
                                TAccessLatency al = rpi.getAccessLatency();
                                if (al != null) {
                                    sb.append(depthPrefix);
                                    sb.append(" access latency:").append(al.getValue());
                                    sb.append('\n');
                                }
                                else {
                                    sb.append(" access latency: null");
                                    sb.append('\n');
                                }
                            }
                            else {
                                sb.append(" retentionpolicyinfo : null");
                                sb.append('\n');
                            }
                            TFileLocality locality =  metaDataPathDetail.getFileLocality();
                            if(locality != null) {
                                sb.append(depthPrefix);
                                sb.append(" locality:").append(locality.getValue());
                                sb.append('\n');
                            }
                            else {
                                sb.append(" locality: null");
                                sb.append('\n');
                            }
                            if (metaDataPathDetail.getCheckSumValue() != null) {
                                sb.append(depthPrefix)
                                        .append(" - Checksum value:  ")
                                        .append(metaDataPathDetail
                                                .getCheckSumValue())
                                        .append('\n');
                            }

                            if (metaDataPathDetail.getCheckSumType() != null) {
                                sb.append(depthPrefix)
                                        .append(" - Checksum type:  ")
                                        .append(metaDataPathDetail
                                                .getCheckSumType())
                                        .append('\n');
                            }
                            SimpleDateFormat df =
                                new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                            FieldPosition tfp =
                                new FieldPosition(DateFormat.FULL);
                            if (metaDataPathDetail.getOwnerPermission() != null) {
                                TUserPermission up =
                                    metaDataPathDetail.getOwnerPermission();
                                sb.append(depthPrefix).append("  UserPermission:");
                                sb.append(" uid=").append( up.getUserID() );
                                sb.append(" Permissions");
                                sb.append(up.getMode().getValue());
                                sb.append('\n');
                            }
                            if (metaDataPathDetail.getGroupPermission() != null) {
                                TGroupPermission gp =
                                    metaDataPathDetail.getGroupPermission();
                                sb.append(depthPrefix).append("  GroupPermission:");
                                sb.append(" gid=").append( gp.getGroupID() );
                                sb.append(" Permissions");
                                sb.append(gp.getMode().getValue());
                                sb.append('\n');
                            }
                            if(metaDataPathDetail.getOtherPermission() != null) {
                                sb.append(depthPrefix).append(" WorldPermission: ");
                                sb.append(metaDataPathDetail.getOtherPermission().getValue());
                                sb.append('\n');
                            }
                            if (metaDataPathDetail.getCreatedAtTime() != null) {
                                Date tdate = metaDataPathDetail.getCreatedAtTime().getTime();
                                if (tdate != null) {
                                    StringBuffer dsb = new StringBuffer();
                                    df.format(tdate, dsb, tfp);
                                    sb.append(depthPrefix).append("created at:").append(dsb);
                                    sb.append('\n');
                                }
                            }
                            if (metaDataPathDetail.getLastModificationTime() != null) {
                                Date tdate =
                                    metaDataPathDetail.getLastModificationTime().getTime();
                                if (tdate != null)  {
                                    StringBuffer dsb = new StringBuffer();
                                    df.format(tdate, dsb, tfp);
                                    sb.append(depthPrefix);
                                    sb.append("modified at:").append(dsb);
                                    sb.append('\n');
                                }
                            }
                            if(metaDataPathDetail.getLifetimeAssigned()!= null) {
                                sb.append(depthPrefix)
                                        .append("  - Assigned lifetime (in seconds):  ")
                                        .append(metaDataPathDetail
                                                .getLifetimeAssigned())
                                        .append('\n');
                            }

                            if(metaDataPathDetail.getLifetimeLeft()!= null) {
                                sb.append(depthPrefix)
                                        .append(" - Lifetime left (in seconds):  ")
                                        .append(metaDataPathDetail
                                                .getLifetimeLeft())
                                        .append('\n');
                            }

                            sb.append(depthPrefix).append(" - Original SURL:  ")
                                    .append(metaDataPathDetail.getPath())
                                    .append('\n').append(" - Status:  ")
                                    .append(metaDataPathDetail.getStatus()
                                            .getExplanation()).append('\n')
                                    .append(" - Type:  ")
                                    .append(metaDataPathDetail.getType())
                                    .append('\n');
                        }
                        if (metaDataPathDetail.getArrayOfSubPaths() != null) {
                            TMetaDataPathDetail subpaths[] =metaDataPathDetail.getArrayOfSubPaths().getPathDetailArray();
                            if(subpaths ==ta) {
                                sb.append(depthPrefix).append( " circular subpath reference !!!");
                            }
                            else {
                                printResults(sb,subpaths,depth+1,depthPrefix+"    ",longFormat);
                            }
                        }
                    }
                }
            }
        }
    }
}


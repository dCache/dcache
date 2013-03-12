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
 * TurlGetterPutter.java
 *
 * Created on May 1, 2003, 12:41 PM
 */

package org.dcache.srm.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import diskCacheV111.srm.ISRM;
import diskCacheV111.srm.RequestFileStatus;
import diskCacheV111.srm.RequestStatus;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.SrmUrl;
/**
 *
 * @author  timur
 */
public abstract class TurlGetterPutterV1 extends TurlGetterPutter {
    private static final Logger logger =
        LoggerFactory.getLogger(TurlGetterPutterV1.class);

    protected ISRM remoteSRM;
    private final Object sync = new Object();
    protected RequestStatus rs;

    // this is the set of remote file IDs that are not "Ready" yet
    // this object will be used for synchronization of all hash sets and maps
    // used in this class
    private  final Collection<Integer> fileIDs = new HashSet<>();

    // The map between remote file IDs and corresponding RequestFileStatuses
    private Map<Integer, RequestFileStatus> fileIDsMap = new HashMap<>();

    // This two maps give the correspondence between local file IDs
    // and a remote file IDs
    protected String SURLs[];
    protected int requestID;
    protected int number_of_file_reqs;
    protected boolean createdMap;
    private long retry_timout;
    private int retry_num;
    private final Transport transport;

    /** Creates a new instance of RemoteTurlGetter */
    public TurlGetterPutterV1(AbstractStorageElement storage,
                              RequestCredential credential, String[] SURLs,
                              String[] protocols,long retry_timeout,int retry_num,
                              Transport transport) {
        super(storage,credential, protocols);
        this.SURLs = SURLs;
        this.number_of_file_reqs = SURLs.length;
        this.retry_num = retry_num;
        this.retry_timout = retry_timeout;
        logger.debug("TurlGetterPutter, number_of_file_reqs = "+number_of_file_reqs);
        this.transport = transport;
    }

    @Override
    public void getInitialRequest() throws SRMException {
        if(number_of_file_reqs == 0) {
            logger.debug("number_of_file_reqs is 0, nothing to do");
            return;
        }
        try {
            //use new client using the Apache Axis SOAP tool
            remoteSRM = new SRMClientV1(
                    new SrmUrl(SURLs[0]),
                    credential.getDelegatedCredential(),
                    retry_timout,retry_num,true,true,
                    transport);
        }
        catch(Exception e) {
            logger.error("failed to connect to {} {}",SURLs[0],e.getMessage());
            throw new SRMException("failed to connect to "+SURLs[0],e);
        }

        logger.debug("run() : calling getInitialRequestStatus()");
        try {
            rs =  getInitialRequestStatus();
        }
        catch(Exception e) {
            throw new SRMException("failed to get initial request status",e);
        }
    }


    @Override
    public void run() {

        if(number_of_file_reqs == 0) {
            logger.debug("number_of_file_reqs is 0, nothing to do");
            return;
        }

        if(rs.fileStatuses == null || rs.fileStatuses.length == 0) {
            String err="run() : fileStatuses "+
            " are null or of zero length";
            notifyOfFailure(err);
            return;
        }
        requestID = rs.requestId;
        RequestFileStatus[] frs = rs.fileStatuses;
        if(frs.length != this.number_of_file_reqs) {
            notifyOfFailure("run(): wrong number of RequestFileStatuses "+frs.length+
                    " should be "+number_of_file_reqs);
            return;
        }

        synchronized(fileIDs) {
            for(int i = 0; i<number_of_file_reqs;++i) {
                Integer fileId = frs[i].fileId;
                fileIDs.add(fileId);

                fileIDsMap.put(fileId,frs[i]);
            }
            createdMap = true;
        }

        logger.debug("getFromRemoteSRM() : received requestStatus, waiting");
        try {
            waitForReadyStatuses();
        }
        catch(Exception e) {
            logger.error(e.toString());
            notifyOfFailure(e);
        }

    }

    private void waitForReadyStatuses() throws Exception{
        while(!fileIDs.isEmpty()) {
            if(isStopped()) {
                logger.debug("TurlGetterPutter is done, still have "+fileIDs.size()+" file ids");
                for (Integer fileID : fileIDs) {
                    RequestFileStatus frs;
                    Integer nextID = fileID;
                    try {
                        logger.debug("calling setFileStatus(" + requestID + "," + nextID + ",\"Done\") on remote server");
                        setFileStatus(requestID, nextID, "Done");
                    } catch (Exception e) {
                        logger.error("error setting file status to done", e);
                    }
                    try {
                        frs = getFileRequest(rs, nextID);
                        notifyOfFailure(frs.SURL, "stopped by user request", Integer
                                .toString(rs.requestId), nextID.toString());
                    } catch (Exception e) {
                        logger.error(e.toString());
                    }
                }
                break;

            } else {
                boolean totalFailure = false;
                String totalFailureError = null;
                Collection<Integer> removeIDs = new HashSet<>();
                HashMap<Integer,Boolean> removedIDsToResutls = new HashMap<>();
                HashMap<Integer,String> removedIDsToSURL = new HashMap<>();
                HashMap<Integer,String> removedIDsToTURL = new HashMap<>();
                HashMap<Integer,Long> removedIDsToSizes = new HashMap<>();
                HashMap<Integer,String> removeIDsToErrorMessages = new HashMap<>();
                synchronized(fileIDs) {

                    for (Integer fileID : fileIDs) {
                        RequestFileStatus frs;
                        Integer nextID = fileID;
                        try {
                            frs = getFileRequest(rs, nextID);
                        } catch (Exception e) {
                            logger.error(e.toString());
                            totalFailure = true;
                            totalFailureError = " run() getFileRequest  failed with ioe=" + e;
                            break;
                        }
                        if (frs == null) {
                            totalFailure = true;
                            totalFailureError = "request status does not have" +
                                    "RequestFileStatus fileID = " + nextID;
                            break;
                        }

                        if (frs.state == null) {
                            totalFailure = true;
                            totalFailureError = "request status does not have state (state is null)" +
                                    "RequestFileStatus fileID = " + nextID;
                            break;
                        }

                        if (frs.state.equals("Pending")) {
                            continue;
                        }

                        logger.debug("waitForReadyStatuses() received the RequestFileStatus with Status=" + frs.state + " for SURL=" + frs.SURL);
                        removeIDs.add(nextID);
                        removedIDsToSURL.put(nextID, frs.SURL);

                        switch (frs.state) {
                        case "Failed":
                            removedIDsToResutls.put(nextID, Boolean.FALSE);
                            removeIDsToErrorMessages
                                    .put(nextID, "remote srm set state to Failed");

                            break;
                        case "Ready":
                        case "Running":
                            if (frs.TURL == null) {
                                removeIDs.add(nextID);
                                removedIDsToResutls.put(nextID, Boolean.FALSE);
                                removeIDsToErrorMessages
                                        .put(nextID, "  TURL nof found but fileStatus state ==" + frs.state);
                            } else {
                                logger.debug("waitForReadyStatuses(): FileRequestStatus is Ready received TURL=" +
                                        frs.TURL);
                                removeIDs.add(nextID);
                                removedIDsToResutls.put(nextID, Boolean.TRUE);
                                removedIDsToTURL.put(nextID, frs.TURL);
                                if (frs.size > 0) {
                                    removedIDsToSizes.put(nextID, frs.size);
                                }
                            }
                            break;
                        case "Done":
                            removedIDsToResutls.put(nextID, Boolean.FALSE);
                            removeIDsToErrorMessages
                                    .put(nextID, "remote srm set state to Done, when we were waiting for Ready");
                            break;
                        default:
                            removedIDsToResutls.put(nextID, Boolean.FALSE);
                            removeIDsToErrorMessages
                                    .put(nextID, "remote srm set state is unknown :" + frs.state
                                            + ", when we were waiting for Ready");
                            break;
                        }
                    }

                    fileIDs.removeAll(removeIDs);

                }

                // we do all notifications outside of the synchronized block to avoid deadlocks
                if(totalFailure){
                    logger.error(" breaking the waiting loop with a failure:"+ totalFailureError);
                    notifyOfFailure(totalFailureError);
                    return;
                }

                for (Integer nextRemoveId : removeIDs) {
                    String surl = removedIDsToSURL.get(nextRemoveId);
                    String turl = removedIDsToTURL.get(nextRemoveId);
                    Long size = removedIDsToSizes.get(nextRemoveId);
                    Boolean success = removedIDsToResutls.get(nextRemoveId);
                    if (success) {
                        notifyOfTURL(surl, turl, Integer
                                .toString(rs.requestId), nextRemoveId
                                .toString(), size);
                    } else {
                        String errormsg = removeIDsToErrorMessages
                                .get(nextRemoveId);
                        notifyOfFailure(surl, errormsg, Integer
                                .toString(rs.requestId), nextRemoveId
                                .toString());
                    }

                }

                synchronized (fileIDs) {

                    if(fileIDs.isEmpty()) {
                        logger.debug("waitForReadyStatuses(): fileIDs is empty, breaking the loop");
                        break;
                    }
                }

                synchronized(sync) {
                    try {
                        int retrytime = rs.retryDeltaTime;
                        if( retrytime <= 0 ) {
                            retrytime = 5;
                        }
                        logger.debug("waitForReadyStatuses(): waiting for "+retrytime+" seconds before updating status ...");
                        sync.wait( retrytime * 1000L );
                    }
                    catch(InterruptedException ie) {
                    }
                }

                synchronized (fileIDs) {
                    if(fileIDs.isEmpty()) {
                        break;
                    }
                }
                rs = getRequestStatus(requestID);

                if(rs == null) {
                    notifyOfFailure(" null requests status");
                    return;
                }

                if(rs.state.equals("Failed")) {
                    logger.error("rs.state = "+rs.state+" rs.error = "+rs.errorMessage);
                    for(int i = 0; i< rs.fileStatuses.length;++i) {
                        logger.error("      ====> fileStatus state =="+rs.fileStatuses[i].state);
                    }
                    notifyOfFailure("rs.state = "+rs.state+" rs.error = "+rs.errorMessage);
                    return;
                }

                if(rs.fileStatuses.length != number_of_file_reqs) {
                    String err= "incorrect number of RequestFileStatuses"+
                    "in RequestStatus expected "+number_of_file_reqs+" received "+rs.fileStatuses.length;
                    notifyOfFailure(err);
                    return;
                }
            }
        }
    }

    private  static RequestFileStatus getFileRequest(RequestStatus rs,Integer nextID) {
        RequestFileStatus[] frs = rs.fileStatuses;
        if(frs == null ) {
            return null;
        }

        for (RequestFileStatus fr : frs) {
            if (fr.fileId == nextID) {
                return fr;
            }
        }
        return null;
    }


    protected abstract RequestStatus getInitialRequestStatus() throws Exception;

    protected RequestStatus getRequestStatus(int requestID) {
        return remoteSRM.getRequestStatus(requestID);
    }

    private  boolean setFileStatus(int requestID,int fileId,String status) {

        RequestStatus srm_status = remoteSRM.setFileStatus(requestID,fileId,status);

        //we are just verifying that the requestId and fileId are valid
        //meaning that the setFileStatus message was received
        if(srm_status == null) {
            return false;
        }
        if(srm_status.requestId != requestID) {
            return false;
        }
        if(srm_status.fileStatuses == null) {
            return false;
        }
        for(int i = 0 ; i <srm_status.fileStatuses.length; ++i) {
            RequestFileStatus fileStatus = srm_status.fileStatuses[i];
            if(fileStatus.fileId == fileId )
            {
                return true;
            }
        }

        return false;
    }


    public static void staticSetFileStatus(RequestCredential credential,
                                           String surl,
                                           int requestID,
                                           int fileId,
                                           String status,
                                           long retry_timeout,
                                           int retry_num,
                                           Transport transport) throws Exception
    {
        ISRM remoteSRM;

        // TODO extract web service path from surl if ?SFN= is present
        remoteSRM = new SRMClientV1(new SrmUrl(surl),
                credential.getDelegatedCredential(),
                retry_timeout, retry_num,true,true,
                transport);

        remoteSRM.setFileStatus(requestID,fileId,status);
    }
}

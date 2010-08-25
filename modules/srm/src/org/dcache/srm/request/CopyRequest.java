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
 * SrmRequestHandler.java
 *
 * Created on January 10, 2003, 1:06 PM
 */

package org.dcache.srm.request;

import org.dcache.srm.util.SrmUrl;
import java.net.MalformedURLException;
import java.io.IOException;
import java.util.Set;
import java.util.Date;
import org.dcache.srm.util.OneToManyMap;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMException;
import org.dcache.srm.util.Tools;
import org.dcache.srm.client.TurlGetterPutter;
import org.dcache.srm.client.TurlGetterPutterV1;
import org.dcache.srm.client.RemoteTurlGetterV1;
import org.dcache.srm.client.RemoteTurlPutterV1;
import org.dcache.srm.client.RemoteTurlGetterV2;
import org.dcache.srm.client.RemoteTurlPutterV2;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.IllegalStateTransition;
import java.beans.PropertyChangeListener;
import org.dcache.srm.client.RequestFailedEvent;
import org.dcache.srm.client.TURLsGetFailedEvent;
import org.dcache.srm.client.TURLsArrivedEvent;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.SRMProtocol;
import org.dcache.srm.qos.*;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMReleasedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author  timur
 */
public final class CopyRequest extends ContainerRequest implements PropertyChangeListener {
    private final static Logger logger =
            LoggerFactory.getLogger(CopyRequest.class);

    private boolean from_url_is_srm;
    private boolean to_url_is_srm;
    private boolean from_url_is_gsiftp;
    private boolean to_url_is_gsiftp;
    private boolean from_url_is_local;
    private boolean to_url_is_local;
    private SrmUrl from_urls[];
    private SrmUrl to_urls[];
    private int number_of_file_reqs;

    private String[] protocols;
    private SRMProtocol callerSrmProtocol;
    private SRMProtocol remoteSrmProtocol;
    private boolean remoteSrmGet;
    private TFileStorageType storageType;
    private final TRetentionPolicy targetRetentionPolicy;
    private final TAccessLatency targetAccessLatency;
    private final TOverwriteMode overwriteMode;
    private String targetSpaceToken;

    private transient final OneToManyMap remoteSurlToFileReqIds = new OneToManyMap();
    private transient TurlGetterPutter getter_putter;
    private transient QOSPlugin qosPlugin = null;

    public CopyRequest( SRMUser user,
    Long requestCredentialId,
    String[] from_urls,
    String[] to_urls,
    String spaceToken,
    long lifetime,
    long max_update_period,
    int max_number_of_retries,
    SRMProtocol callerSrmProtocol,
    TFileStorageType storageType,
    TRetentionPolicy targetRetentionPolicy,
    TAccessLatency targetAccessLatency,
    String description,
    String client_host,
    TOverwriteMode overwriteMode
    ) throws Exception{
        super(user,
            requestCredentialId,
                max_number_of_retries,
                max_update_period,
                lifetime,
                description,
                client_host);
        java.util.ArrayList prot_list = new java.util.ArrayList(4);

        if(getConfiguration().isUseGsiftpForSrmCopy()) {
            prot_list.add("gsiftp");
        }
        if(getConfiguration().isUseHttpForSrmCopy()) {
            prot_list.add("http");
        }
        if(getConfiguration().isUseDcapForSrmCopy()) {
            prot_list.add("dcap");
        }
        if(getConfiguration().isUseFtpForSrmCopy()) {
            prot_list.add("ftp");
        }

        protocols = (String[]) prot_list.toArray(new String[0]);
        int reqs_num = from_urls.length;
        if(reqs_num != to_urls.length) {
            logger.error("Request createCopyRequest : "+
            "unequal number of elements in url arrays");
            throw new IllegalArgumentException(
            "unequal number of elements in url arrays");
        }
        fileRequests = new FileRequest[reqs_num];
        for(int i = 0; i<reqs_num; ++i) {
            CopyFileRequest fileRequest =
                new CopyFileRequest(getId(),
                requestCredentialId,from_urls[i],to_urls[i],
                spaceToken,
                lifetime, max_number_of_retries  );
            fileRequests[i] = fileRequest;
        }
        this.callerSrmProtocol = callerSrmProtocol;
        if (getConfiguration().getQosPluginClass()!=null) {
            this.qosPlugin = QOSPluginFactory.createInstance(getConfiguration());
        }
        this.storageType = storageType;
        this.targetAccessLatency = targetAccessLatency;
        this.targetRetentionPolicy = targetRetentionPolicy;
        this.overwriteMode = overwriteMode;
        this.targetSpaceToken = spaceToken;
        updateMemoryCache();
        logger.debug("Request.createCopyRequest : created new request succesfully");
    }

    /**
     * restore constructor
     */
    public  CopyRequest(
    Long id,
    Long nextJobId,
    long creationTime,
    long lifetime,
    int stateId,
    String errorMessage,
    SRMUser user,
    String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    Long credentialId,
    FileRequest[] fileRequest,
    int retryDeltaTime,
    boolean should_updateretryDeltaTime,
    String description,
    String client_host,
    String statusCodeString,
    TFileStorageType storageType,
    TRetentionPolicy targetRetentionPolicy,
    TAccessLatency targetAccessLatency)  throws java.sql.SQLException {
        super( id,
        nextJobId,
        creationTime,
        lifetime,
        stateId,
        errorMessage,
        user,
        scheduelerId,
        schedulerTimeStamp,
        numberOfRetries,
        maxNumberOfRetries,
        lastStateTransitionTime,
        jobHistoryArray,
        credentialId,
        fileRequest,
        retryDeltaTime,
        should_updateretryDeltaTime,
        description,
        client_host,
        statusCodeString);

        java.util.ArrayList prot_list = new java.util.ArrayList(4);

        if(getConfiguration().isUseGsiftpForSrmCopy()) {
            prot_list.add("gsiftp");
        }
        if(getConfiguration().isUseHttpForSrmCopy()) {
            prot_list.add("http");
        }
        if(getConfiguration().isUseDcapForSrmCopy()) {
            prot_list.add("dcap");
        }
        if(getConfiguration().isUseFtpForSrmCopy()) {
            prot_list.add("ftp");
        }

        protocols = (String[]) prot_list.toArray(new String[0]);
        if (getConfiguration().getQosPluginClass()!=null) {
            this.qosPlugin = QOSPluginFactory.createInstance(getConfiguration());
        }
        this.storageType = storageType;
        this.targetAccessLatency = targetAccessLatency;
        this.targetRetentionPolicy = targetRetentionPolicy;
        this.overwriteMode = null;
     }



    public int getNumOfFileRequest() {
        if(fileRequests == null) {
            return 0;
        }
        return fileRequests.length;
    }


    public void proccessRequest()  throws java.sql.SQLException,Exception {

        logger.debug("Proccessing request");
        if(fileRequests == null || fileRequests.length == 0) {
            try {
                setState(State.FAILED,"Request contains zero file requests");
                return;
            }
            catch(IllegalStateTransition ist)
            {
                logger.error("Illegal State Transition : " +ist.getMessage());
            }

        }
        setNumber_of_file_reqs(fileRequests.length);
        logger.debug("number_of_file_reqs = "+getNumber_of_file_reqs());
        wlock();
        try {
            from_urls = new SrmUrl[getNumber_of_file_reqs()];
            to_urls = new SrmUrl[getNumber_of_file_reqs()];
            for(int i = 0 ; i<getNumber_of_file_reqs();++i) {
                CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];

                from_urls[i] = new SrmUrl(cfr.getFromURL());
                to_urls[i] = new SrmUrl(cfr.getToURL());
            }
        } catch(MalformedURLException murle) {
            logger.error(murle.toString());
            try {
                setState(State.FAILED, murle.toString());
            }
            catch(IllegalStateTransition ist)
            {
                logger.error("Illegal State Transition : " +ist.getMessage());
            }
            return;

        } finally {
            wunlock();
        }
        identify();
        getTURLs();
    }

    private void identify() throws IOException, SRMException {
        wlock();
        try {
            for(int i=1;i<getNumber_of_file_reqs();++i) {
                if(
                ! from_urls[i].getProtocol().equals(from_urls[0].getProtocol()) ||
                ! from_urls[i].getHost().equals(from_urls[0].getHost()) ||
                ! (from_urls[i].getPort() == from_urls[0].getPort()) ) {
                    String err = "source url #"+i+" "+from_urls[i].getURL()+" and "+
                    "source url #0"+from_urls[0].getURL()+" are not compartible";
                    logger.error(err.toString());
                    throw new IOException(err);
                }

                if(
                ! to_urls[i].getProtocol().equals(to_urls[0].getProtocol()) ||
                ! to_urls[i].getHost().equals(to_urls[0].getHost()) ||
                ! (to_urls[i].getPort() == to_urls[0].getPort()) ) {
                    String err = "dest url #"+i+" "+to_urls[i].getURL()+" and "+
                    "dest url #0"+to_urls[0].getURL()+" are not compartible";
                    logger.error(err.toString());
                    throw new IOException(err);
                }

            }

            from_url_is_srm = from_urls[0].getProtocol().equals("srm");
            to_url_is_srm = to_urls[0].getProtocol().equals("srm");
            from_url_is_gsiftp = from_urls[0].getProtocol().equals("gsiftp");
            to_url_is_gsiftp = to_urls[0].getProtocol().equals("gsiftp");

            if(isFrom_url_is_srm()) {
                int srm_port = getConfiguration().getPort();
                int from_url_port = from_urls[0].getPort();
                if(srm_port == from_url_port) {
                    from_url_is_local =
                            Tools.sameHost(getConfiguration().getSrmHosts(),
                            from_urls[0].getHost());
                }
            }
            else {
                from_url_is_local = getStorage().isLocalTransferUrl(
                        from_urls[0].getURL());
            }

            if(to_url_is_srm) {
                int srm_port = getConfiguration().getPort();
                int to_url_port = to_urls[0].getPort();
                if(srm_port == to_url_port) {
                    to_url_is_local = Tools.sameHost(
                            getConfiguration().getSrmHosts(),
                            to_urls[0].getHost());
                }
            }
            else {
                to_url_is_local = getStorage().isLocalTransferUrl(
                        to_urls[0].getURL());
            }

            logger.debug(" from_url_is_srm = "+from_url_is_srm);
            logger.debug(" to_url_is_srm = "+to_url_is_srm);
            logger.debug(" from_url_is_gsiftp = "+from_url_is_gsiftp);
            logger.debug(" to_url_is_gsiftp = "+to_url_is_gsiftp);
            logger.debug(" from_url_is_local = "+from_url_is_local);
            logger.debug(" to_url_is_local = "+to_url_is_local);

            if(!from_url_is_local && ! to_url_is_local) {
                logger.error("both from and to url are not local srm");
                throw new IOException("both from and to url are not local srm");
            }
        } finally {
            wunlock();
        }
    }

    private void makeQosReservation(int i) throws MalformedURLException, SRMException {
        try {
            CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
            RequestCredential credential = getCredential();
            QOSTicket qosTicket = getQosPlugin().createTicket(
                    credential.getCredentialName(),
                    (getStorage().getFileMetaData((SRMUser)getUser(),cfr.getFromPath(), false)).size,
                    getFrom_url(i).getURL(),
                    getFrom_url(i).getPort(),
                    getFrom_url(i).getPort(),
                    getFrom_url(i).getProtocol(),
                    getTo_url(i).getURL(),
                    getTo_url(i).getPort(),
                    getTo_url(i).getPort(),
                    getTo_url(i).getProtocol());
            getQosPlugin().addTicket(qosTicket);
            if (getQosPlugin().submit()) {
                cfr.setQOSTicket(qosTicket);
                logger.debug("QOS Ticket Received "+getQosPlugin().toString());
            }
        } catch(Exception e) {
            logger.error("Could not create QOS reservation: "+e.getMessage());
        }
    }

    private void getTURLs() throws SRMException,
    IOException,InterruptedException,IllegalStateTransition ,java.sql.SQLException,
    org.dcache.srm.scheduler.FatalJobFailure {
        logger.debug("getTURLS()");
        if(isFrom_url_is_srm() && ! isFrom_url_is_local()) {
            // this means that the from url is remote srm url
            // and a "to" url is a local srm url
            if(getStorageType() != null && !storageType.equals(TFileStorageType.PERMANENT)) {
                  throw new
                      org.dcache.srm.scheduler.FatalJobFailure(
                      "TargetFileStorageType "+getStorageType()+" is not supported");
            }
            RequestCredential credential = getCredential();
            logger.debug("obtained credential="+credential+" id="+credential.getId());
            //String ls_client = "SRM"; // make it not hard coded

            for(int i = 0 ; i<getNumber_of_file_reqs();++i) {
                CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }
                if(cfr.getFrom_turl() != null ) {
                    Scheduler.getScheduler(schedulerId).schedule(cfr);
                    continue;
                }

                //Since "to" url has to be local srm, we can just set the local to path
                remoteSurlToFileReqIds.put(getFrom_url(i).getURL(),fileRequests[i].getId());
                logger.debug("getTurlsArrived, setting local \"to\" path to "+
                cfr.getToPath());
                cfr.setLocal_to_path(
                cfr.getToPath());
                cfr.saveJob();
            }
            String[] remoteSurlsUniqueArray =
            (String[]) remoteSurlToFileReqIds.keySet().toArray(new String[0]);
            for(int i=0;i<remoteSurlsUniqueArray.length;++i) {
                logger.debug("remoteSurlsUniqueArray["+i+"]="+remoteSurlsUniqueArray[i]);
            }
            //need to get from remote srm system
            setRemoteSrmGet(true);
            if(getCallerSrmProtocol() == null || getCallerSrmProtocol() == SRMProtocol.V1_1) {
                 try {
                    setGetter_putter(new RemoteTurlGetterV1(getStorage(), credential, remoteSurlsUniqueArray, getProtocols(), this, getConfiguration().getCopyRetryTimeout(), 2));
                    getGetter_putter().getInitialRequest();
                    setRemoteSrmProtocol(SRMProtocol.V1_1);
                 }
                 catch(SRMException srme) {
                     logger.error("connecting to server using version 1.1 protocol failed, trying version 2.1.1");
                    setGetter_putter(new RemoteTurlGetterV2(getStorage(), credential, remoteSurlsUniqueArray, getProtocols(), this, getConfiguration().getCopyRetryTimeout(), 2, this.getRemainingLifetime()));
                    getGetter_putter().getInitialRequest();
                    setRemoteSrmProtocol(SRMProtocol.V2_1);
                 }
            } else if ( getCallerSrmProtocol() == SRMProtocol.V2_1) {
                 try{
                    setGetter_putter(new RemoteTurlGetterV2(getStorage(), credential, remoteSurlsUniqueArray, getProtocols(), this, getConfiguration().getCopyRetryTimeout(), 2, this.getRemainingLifetime()));
                    getGetter_putter().getInitialRequest();
                    setRemoteSrmProtocol(SRMProtocol.V2_1);
                 }
                 catch(SRMException srme) {
                     logger.error("connecting to server using version 2.1.1 protocol failed, trying version 1.1");
                    setGetter_putter(new RemoteTurlGetterV1(getStorage(), credential, remoteSurlsUniqueArray, getProtocols(), this, getConfiguration().getCopyRetryTimeout(), 2));
                    getGetter_putter().getInitialRequest();
                    setRemoteSrmProtocol(SRMProtocol.V1_1);
                 }

             } else {
                 throw new org.dcache.srm.scheduler.FatalJobFailure("usupported srm protocol");
             }

            getGetter_putter().run();

             return;

         }

        if(isFrom_url_is_srm()) // from_url_is_local is true (nonlocal case handled above)
         {
             // this means that the from url is loacal srm url.
            for(int i = 0;i<this.getNumber_of_file_reqs();++i) {
                CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }

                if(cfr.getLocal_from_path() != null ) {
                    continue;
                }
                logger.debug("getTurlsArrived, setting local \"from\" path to "+
                cfr.getFromPath());
                cfr.setLocal_from_path(
                cfr.getFromPath());
                cfr.saveJob();

            }
        }
        else {
            // from url is not srm url
            // we have a remote transfer url  as source and
            // a local srm url as destination
            for(int i = 0;i<this.getNumber_of_file_reqs();++i) {
                CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }

                if(cfr.getFrom_turl() != null ) {
                    continue;
                }
                logger.debug("getTurlsArrived, setting \"from\" turl to "+
                getFrom_url(i).getURL());
                cfr.setFrom_turl(getFrom_url(i));
                cfr.saveJob();

            }

        }

        // now "from" turl or local path is known, need to handle the "to" part

        if(isTo_url_is_srm() &&  isTo_url_is_local()) {
            //this means that we either local "from" srm url or
            // non local "from" turl, and we have local to srm
            // we have all info needed to proccede
            for(int i = 0;i<this.getNumber_of_file_reqs();++i) {
                CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }

                logger.debug("getTurlsArrived, setting local \"to\" path to "+
                cfr.getToPath());
                cfr.setLocal_to_path(
                cfr.getToPath());
                cfr.saveJob();
                // everything is known, can start transfers
                Scheduler.getScheduler(schedulerId).schedule(cfr);
            }
            return;
        }

        if(!isTo_url_is_srm()) {
            // this means we have local from url and some "to" turl that is given
            // we have all info needed to proccede
            for(int i = 0;i<this.getNumber_of_file_reqs();++i) {
                CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }
                logger.debug("getTurlsArrived, setting remote \"to\" TURL to "+
                getTo_url(i).getURL());
                cfr.setTo_turl(getTo_url(i));
                cfr.saveJob();
                // everything is known, can start transfers
                Scheduler.getScheduler(schedulerId).schedule(cfr);
            }
            return;
        }
        // the only case remaining is the local "from" srm url
        //the to url is a remote srm url -> need to discover "to" TURL

        for(int i = 0;i<this.getNumber_of_file_reqs();++i) {
            CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
            if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                // copy file request has being canceled,failed or scheduled before
                continue;
            }

            if(cfr.getTo_turl() != null ) {
                //to turl has arrived , but request has not been scheduled
                Scheduler.getScheduler(schedulerId).schedule(cfr);
                continue;
            }

            remoteSurlToFileReqIds.put(getTo_url(i).getURL(),fileRequests[i].getId());
        }

        String[] remoteSurlsUniqueArray =
        (String[]) remoteSurlToFileReqIds.keySet().toArray(new String[0]);
        int length = remoteSurlsUniqueArray.length;
        String[] dests = new String[length];
        long[] sizes = new long[length];
        for(int i =0 ; i<length;++i) {
            Long fileRequestId = (Long) remoteSurlToFileReqIds.get(remoteSurlsUniqueArray[i]);
            CopyFileRequest cfr = (CopyFileRequest)getFileRequest(fileRequestId);
            sizes[i] = (getStorage().getFileMetaData(getUser(),cfr.getFromPath(), false)).size;
            logger.debug("getTURLs: local size  returned by storage.getFileMetaData is "+sizes[i]);
            cfr.setSize(sizes[i]);
            dests[i] = cfr.getToURL();
            if (getQosPlugin() != null) {
                makeQosReservation(i);
            }
         }

        setRemoteSrmGet(false);
         //need to put into the remote srm system
         RequestCredential credential = getCredential();
       if(getCallerSrmProtocol() == null || getCallerSrmProtocol() == SRMProtocol.V1_1) {
            try {
                setGetter_putter(new RemoteTurlPutterV1(getStorage(), credential, dests, sizes, getProtocols(), this, getConfiguration().getCopyRetryTimeout(), 2 ));
                getGetter_putter().getInitialRequest();
                setRemoteSrmProtocol(SRMProtocol.V1_1);
             }
             catch(SRMException srme) {
                 logger.error("connecting to server using version 1.1 protocol failed, trying version 2.1.1");
                 setGetter_putter(new RemoteTurlPutterV2(getStorage(), credential, dests, sizes, getProtocols(), this, getConfiguration().getCopyRetryTimeout(), 2, this.getRemainingLifetime(), getStorageType(), getTargetRetentionPolicy(), getTargetAccessLatency(), getOverwriteMode(), getTargetSpaceToken()));
                 getGetter_putter().getInitialRequest();
                 setRemoteSrmProtocol(SRMProtocol.V2_1);
             }
        } else if ( getCallerSrmProtocol() == SRMProtocol.V2_1) {
            try {
                setGetter_putter(new RemoteTurlPutterV2(getStorage(), credential, dests, sizes, getProtocols(), this, getConfiguration().getCopyRetryTimeout(), 2, this.getRemainingLifetime(), getStorageType(), getTargetRetentionPolicy(), getTargetAccessLatency(), getOverwriteMode(), getTargetSpaceToken()));
                getGetter_putter().getInitialRequest();
                setRemoteSrmProtocol(SRMProtocol.V2_1);
             }
             catch(SRMException srme) {
                 logger.error("connecting to server using version 2.1.1 protocol failed, trying version 1.1");
                setGetter_putter(new RemoteTurlPutterV1(getStorage(), credential, dests, sizes, getProtocols(), this, getConfiguration().getCopyRetryTimeout(), 2));
                getGetter_putter().getInitialRequest();
                setRemoteSrmProtocol(SRMProtocol.V1_1);
             }
         } else {
             throw new org.dcache.srm.scheduler.FatalJobFailure("usupported srm protocol");
         }

         getGetter_putter().run();
         return;
    }

    public void turlArrived(String SURL, String TURL,String remoteRequestId,String remoteFileId,Long size)  throws java.sql.SQLException {

        synchronized(remoteSurlToFileReqIds) {
            Set fileRequestSet = remoteSurlToFileReqIds.getValues(SURL);
            if(fileRequestSet == null || fileRequestSet.isEmpty()) {
                logger.error("turlArrived for unknown SURL = "+SURL+" !!!!!!!");
                return;
            }
            Long[] cfr_ids = (Long[])fileRequestSet.toArray(new Long[0]);
            Date now = new Date();
            long t = now.getTime();
            for(int i = 0 ;i< cfr_ids.length;i++) {

                CopyFileRequest cfr  = (CopyFileRequest)getFileRequest(cfr_ids[i]);
                if (getQosPlugin()!=null && cfr.getQOSTicket()!=null) {
                    getQosPlugin().sayStatus(cfr.getQOSTicket());
                }

                try {
                    if( isFrom_url_is_srm() && ! isFrom_url_is_local()) {
                        cfr.setFrom_turl(new SrmUrl(TURL));
                    }
                    else {
                        cfr.setTo_turl(new SrmUrl(TURL));
                    }
                    if(size != null)
                    {
                        cfr.setSize(size.longValue());
                    }
                }
                catch(MalformedURLException mue) {

                }
                cfr.setRemoteRequestId(remoteRequestId);
                cfr.setRemoteFileId(remoteFileId);
                cfr.saveJob();


                try {
                    String theShedulerId = schedulerId;
                    State file_state = cfr.getState();
                    if(theShedulerId != null &&
                        !(file_state == State.CANCELED || file_state == State.FAILED ||file_state == State.DONE) )
                    {
                        Scheduler.getScheduler(theShedulerId).schedule(cfr);
                    }
                }
                catch(Exception e) {
                    logger.error(e.toString());
                    logger.error("failed to schedule CopyFileRequest " +cfr);
                    try {
                        cfr.setState(State.FAILED,"failed to schedule CopyFileRequest " +cfr +" rasaon: "+e);
                    }
                    catch(IllegalStateTransition ist)
                    {
                        logger.error("Illegal State Transition : " +ist.getMessage());
                    }
                }
                remoteSurlToFileReqIds.remove(SURL,cfr_ids[i]);
            }
        }
    }

    public void turlRetrievalFailed(String SURL, String reason,String remoteRequestId,String remoteFileId)  throws java.sql.SQLException {

        synchronized(remoteSurlToFileReqIds) {
            Set fileRequestSet = remoteSurlToFileReqIds.getValues(SURL);
            if(fileRequestSet == null || fileRequestSet.isEmpty()) {
                logger.error("turlArrived for unknown SURL = "+SURL);
                return;
            }
            Long[] cfr_ids = (Long[])fileRequestSet.toArray(new Long[0]);
            for(int i = 0 ;i< cfr_ids.length;i++) {

                CopyFileRequest cfr  = (CopyFileRequest)getFileRequest(cfr_ids[i]);

                try {
                    String error;
                    if( isFrom_url_is_srm() && ! isFrom_url_is_local()) {
                        error = "retrieval of \"from\" TURL failed with error "+reason;
                    }
                    else {
                        error = "retrieval of \"to\" TURL failed with error "+reason;
                    }
                    logger.error(error.toString());
                    cfr.setState(State.FAILED,error);
                }
                catch(IllegalStateTransition ist) {
                    logger.error("Illegal State Transition : " +ist.getMessage());
                }
                cfr.saveJob();

                remoteSurlToFileReqIds.remove(SURL,cfr_ids[i]);
            }
        }
        remoteFileRequestDone(SURL,remoteRequestId,remoteFileId);

    }

    public void turlsRetrievalFailed(Object reason)  throws java.sql.SQLException {
        synchronized(remoteSurlToFileReqIds) {
            String SURLs[] = (String[] )remoteSurlToFileReqIds.keySet().toArray(new String[0]);
            for( int i = 0;
                 i <SURLs.length;++i)
            {
                String SURL = SURLs[i];
                Set fileRequestSet = remoteSurlToFileReqIds.getValues(SURL);
                Long[] cfr_ids = (Long[])fileRequestSet.toArray(new Long[0]);
                for(int j = 0 ;j< cfr_ids.length;j++) {
                    CopyFileRequest cfr  = (CopyFileRequest)getFileRequest(cfr_ids[j]);
                    try {
                        String error;
                        if( isFrom_url_is_srm() && ! isFrom_url_is_local()) {

                            error = "retrieval of \"from\" TURL failed with error "+reason;
                        }
                        else {
                            error = "retrieval of \"to\" TURL failed with error "+reason;
                        }
                        logger.error(error.toString());
                        cfr.setState(State.FAILED,error);
                    }
                    catch(IllegalStateTransition ist) {
                        logger.error("Illegal State Transition : " +ist.getMessage());
                    }
                    cfr.saveJob();
                    remoteSurlToFileReqIds.remove(SURL,cfr_ids[j]);
                }
            }
        }
    }

    public void remoteFileRequestDone(String SURL,
    String remoteRequestIdString,
    String remoteFileIdString) {
        synchronized(remoteSurlToFileReqIds) {
            try {
                if(getRemoteSrmProtocol() == SRMProtocol.V1_1) {
                    int remoteRequestId = Integer.parseInt(remoteRequestIdString);
                    int remoteFileId = Integer.parseInt(remoteFileIdString);
                    TurlGetterPutterV1.staticSetFileStatus(getCredential(),SURL,
                    remoteRequestId, remoteFileId,"Done",
                getConfiguration().getCopyRetryTimeout(),
                            getConfiguration().getCopyMaxNumOfRetries());
                } else if( getRemoteSrmProtocol() == SRMProtocol.V2_1) {
                    if(isRemoteSrmGet())
                    {
                       RemoteTurlGetterV2.staticReleaseFile(getCredential(),
                               SURL,
                               remoteRequestIdString,
                           getConfiguration().getCopyRetryTimeout(),
                           getConfiguration().getCopyMaxNumOfRetries());
                    } else {
                        RemoteTurlPutterV2.staticPutDone(getCredential(),
                               SURL,
                               remoteRequestIdString,
                           getConfiguration().getCopyRetryTimeout(),
                           getConfiguration().getCopyMaxNumOfRetries());
                    }

                } else {
                    logger.error("unknown or null callerSrmProtocol");
                }
            }
            catch(Exception e) {
                logger.error("set remote file status to done failed, surl = "+SURL+
                " requestId = " +remoteRequestIdString+ " fileId = " +remoteFileIdString);
            }
        }
    }



    public final CopyFileRequest getFileRequestBySurls(String fromurl,String tourl)
    throws java.sql.SQLException, SRMException{
        if(fromurl == null || tourl == null) {
           throw new SRMException("surl is null");
        }
        for(int i =0; i<fileRequests.length;++i) {
            if(((CopyFileRequest)fileRequests[i]).getFromURL().equals(fromurl) &&
               ((CopyFileRequest)fileRequests[i]).getToURL().equals(tourl) ) {
                return (CopyFileRequest)fileRequests[i];
            }
        }
        throw new SRMException("file request for from url ="+fromurl+
        " and to url="+tourl +" is not found");
    }


    public String getMethod() {
        return "Copy";
    }

    //we want to stop handler if the
    //the request is ready (all file reqs are ready), since all copy transfers are
    // competed by now
    public boolean shouldStopHandlerIfReady() {
        return true;
    }

   private volatile boolean processingDone = false;

   private static final long serialVersionUID = 7528188091894319055L;

    public void run() throws org.dcache.srm.scheduler.NonFatalJobFailure, org.dcache.srm.scheduler.FatalJobFailure {
        if(isProcessingDone())
        {
            return;
        }
        try
        {
            proccessRequest();
            boolean done = true;
            for(int i = 0; i< fileRequests.length; ++i) {
                FileRequest fr = fileRequests[i];
                State state = fr.getState();
                if(!(State.isFinalState(state))) {
                    done = false;
                }
            }

            setProcessingDone(true);
            if(done) {
                setState(State.DONE,"all file request completed");
            }
            else {
                setState(State.ASYNCWAIT, "waiting for files to complete");
            }
        }
        catch(org.dcache.srm.scheduler.FatalJobFailure fje) {
            throw fje;
        }
        catch(Exception e)
        {
            logger.error(e.toString());
            logger.error("throwing nonfatal exception for retry");
            throw new org.dcache.srm.scheduler.NonFatalJobFailure(e.toString());
        }

    }

    protected void stateChanged(org.dcache.srm.scheduler.State oldState) {
        State state = getState();
        if(State.isFinalState(state)) {

            TurlGetterPutter a_getter_putter = getGetter_putter();
            if(a_getter_putter != null) {
                logger.error("copyRequest getter_putter is non null, stopping");
                a_getter_putter.stop();
            }
            logger.debug("copy request state changed to "+state);
            for(int i = 0 ; i < fileRequests.length; ++i) {
                try {
                    FileRequest fr = fileRequests[i];
                    State fr_state = fr.getState();
                    if(!(State.isFinalState(fr_state)))
                    {

                        logger.error("changing fr#"+fileRequests[i].getId()+" to "+state);
                            fr.setState(state,"Request state changed, changing file state");
                    }
                }
                catch(IllegalStateTransition ist) {
                    logger.error("Illegal State Transition : " +ist.getMessage());
                }
            }

        }

    }

    public void propertyChange(java.beans.PropertyChangeEvent evt) {
        logger.debug("propertyChange");
        try {
            if(evt instanceof TURLsArrivedEvent) {

                TURLsArrivedEvent tae = (TURLsArrivedEvent) evt;
                String SURL = tae.getSURL();
                String TURL = tae.getTURL();
                String remoteRequestId = tae.getRequestId();
                String remoteFileId = tae.getFileRequestId();
                Long size= tae.getSize();
                turlArrived(SURL, TURL, remoteRequestId,remoteFileId,size);

            }
            else if (evt instanceof TURLsGetFailedEvent)
            {
                TURLsGetFailedEvent tgfe = (TURLsGetFailedEvent)evt;
                String SURL = tgfe.getSURL();
                String reason = tgfe.getReason();
                String remoteRequestId = tgfe.getRequestId();
                String remoteFileId = tgfe.getFileRequestId();
                turlRetrievalFailed(SURL, reason, remoteRequestId, remoteFileId);
            }
            else if(evt instanceof RequestFailedEvent)
            {
                RequestFailedEvent rfe = (RequestFailedEvent)evt;
                Object reason = rfe.getReason();
                turlsRetrievalFailed(reason);

            }

        }catch(Exception e) {
            logger.error(e.toString());
        }
    }

    public void fileRequestCompleted()
    {
        resetRetryDeltaTime();

        if(isProcessingDone())
        {

            try
            {
                boolean done = true;
                for(int i = 0; i< fileRequests.length; ++i) {
                    FileRequest fr = fileRequests[i];
                    State state = fr.getState();
                    if(!(State.isFinalState(state ))) {
                        done = false;
                    }
                }

                State state = getState();
                if(!State.isFinalState(state)) {
                    if(done) {
                        setState(State.DONE,"all files requests have completed ");
                    }
                }
            }
            catch(Exception e)
            {
                logger.error(e.toString());
                logger.error("setting to done anyway");
                try
                {
                    State state = getState();
                    if(!State.isFinalState(state)) {
                        setState(State.DONE,e.toString());
                    }
                }
                catch(IllegalStateTransition ist)
                {
                    logger.error("Illegal State Transition : " +ist.getMessage());
                }
            }
        }
    }

    public final SrmCopyResponse getSrmCopyResponse()
        throws SRMException ,java.sql.SQLException {
        SrmCopyResponse response = new SrmCopyResponse();
        response.setReturnStatus(getTReturnStatus());
        response.setRequestToken(getTRequestToken());
        ArrayOfTCopyRequestFileStatus arrayOfTCopyRequestFileStatus =
            new ArrayOfTCopyRequestFileStatus();
        arrayOfTCopyRequestFileStatus.setStatusArray(
            getArrayOfTCopyRequestFileStatuses(null,null));
        response.setArrayOfFileStatuses(arrayOfTCopyRequestFileStatus);
        return response;
    }


    private String getTRequestToken() {
        return getId().toString();
    }

    public final TCopyRequestFileStatus[]  getArrayOfTCopyRequestFileStatuses(
        String[] fromurls,String[] tourls)
        throws SRMException, java.sql.SQLException {
            if(fromurls != null) {
               if(tourls == null ||
                  fromurls.length != tourls.length ) {
                      throw new SRMException("incompatible fromurls and tourls arrays");

               }
            }
            int len = fromurls == null ? getNumOfFileRequest():fromurls.length;
            TCopyRequestFileStatus[] copyRequestFileStatuses =
             new TCopyRequestFileStatus[len];
            if(fromurls == null) {
                for(int i = 0; i< len; ++i) {
                    CopyFileRequest fr =(CopyFileRequest)fileRequests[i];
                    copyRequestFileStatuses[i] = fr.getTCopyRequestFileStatus();
                }
            } else {
                for(int i = 0; i< len; ++i) {
                    CopyFileRequest fr = getFileRequestBySurls(fromurls[i],tourls[i]);
                    copyRequestFileStatuses[i] = fr.getTCopyRequestFileStatus();
                }

            }
            return copyRequestFileStatuses;
        }

    public final SrmStatusOfCopyRequestResponse getSrmStatusOfCopyRequest()
        throws SRMException, java.sql.SQLException {
        return getSrmStatusOfCopyRequest(null,null);
    }

    public final SrmStatusOfCopyRequestResponse getSrmStatusOfCopyRequest(String[] fromurls,String[] tourls)
        throws SRMException, java.sql.SQLException {
        SrmStatusOfCopyRequestResponse response = new SrmStatusOfCopyRequestResponse();
        response.setReturnStatus(getTReturnStatus());
        ArrayOfTCopyRequestFileStatus arrayOfTCopyRequestFileStatus =
            new ArrayOfTCopyRequestFileStatus();
        arrayOfTCopyRequestFileStatus.setStatusArray(
            getArrayOfTCopyRequestFileStatuses(fromurls,tourls));
        response.setArrayOfFileStatuses(arrayOfTCopyRequestFileStatus);
        return response;
    }

    public final FileRequest getFileRequestBySurl(String surl) throws java.sql.SQLException, SRMException {
        if(surl == null ) {
           throw new SRMException("surl is null");
        }
        for(int i =0; i<fileRequests.length;++i) {
            if(((CopyFileRequest)fileRequests[i]).getFromURL().equals(surl) ||
               ((CopyFileRequest)fileRequests[i]).getToURL().equals(surl) ) {
                return fileRequests[i];
            }
        }
        throw new SRMException("file request for url ="+surl+
        " is not found");
    }

    public final TSURLReturnStatus[] getArrayOfTSURLReturnStatus(String[] surls) throws SRMException, java.sql.SQLException {
        int len ;
        TSURLReturnStatus[] surlLReturnStatuses;
        if(surls == null) {
            len = getNumOfFileRequest();
           surlLReturnStatuses = new TSURLReturnStatus[len];
        }
        else {
            len = surls.length;
           surlLReturnStatuses = new TSURLReturnStatus[surls.length];
        }
        if(surls == null) {
            for(int i = 0; i< len; ++i) {
                CopyFileRequest fr =(CopyFileRequest)fileRequests[i];
                surlLReturnStatuses[i] = fr.getTSURLReturnStatus( null);
            }
        } else {
            for(int i = 0; i< len; ++i) {
                CopyFileRequest fr =(CopyFileRequest)getFileRequestBySurl(surls[i]);
                surlLReturnStatuses[i] = fr.getTSURLReturnStatus(surls[i]);
            }

        }
        return surlLReturnStatuses;
    }

    public TRequestType getRequestType() {
        rlock();
        try {
            return TRequestType.COPY;
        } finally {
            runlock();
        }
    }

    public TFileStorageType getStorageType() {
        rlock();
        try {
            return storageType;
        } finally {
            runlock();
        }
    }

    public void setStorageType(TFileStorageType storageType) {
        wlock();
        try {
            this.storageType = storageType;
        } finally {
            wunlock();
        }
    }

    public TRetentionPolicy getTargetRetentionPolicy() {
        return targetRetentionPolicy;
    }

    public TAccessLatency getTargetAccessLatency() {
        return targetAccessLatency;
    }

    public TOverwriteMode getOverwriteMode() {
        return overwriteMode;
    }

    public boolean isOverwrite() {
        if(getConfiguration().isOverwrite()) {
            if(getOverwriteMode() == null) {
                return getConfiguration().isOverwrite_by_default();
            }
            return getOverwriteMode().equals(TOverwriteMode.ALWAYS);
        }
        return false;
    }

    @Override
    public long extendLifetimeMillis(long newLifetimeInMillis) throws SRMException {
        try {
            return super.extendLifetimeMillis(newLifetimeInMillis);
        } catch(SRMReleasedException releasedException) {
            throw new SRMInvalidRequestException(releasedException.getMessage());
        }
    }

    /**
     * @return the from_url_is_srm
     */
    private boolean isFrom_url_is_srm() {
        rlock();
        try {
            return from_url_is_srm;
        } finally {
            runlock();
        }
    }

    /**
     * @return the to_url_is_srm
     */
    private boolean isTo_url_is_srm() {
        rlock();
        try {
            return to_url_is_srm;
        } finally {
            runlock();
        }
    }

    /**
     * @return the from_url_is_local
     */
    private boolean isFrom_url_is_local() {
        rlock();
        try {
            return from_url_is_local;
        } finally {
            runlock();
        }
    }

    /**
     * @return the to_url_is_local
     */
    private boolean isTo_url_is_local() {
        rlock();
        try {
            return to_url_is_local;
        } finally {
            runlock();
        }
    }

    /**
     * @param i indext of the "from" url in the array
     * @return the from_urls
     */
    private SrmUrl getFrom_url(int i) {
        rlock();
        try {
            return from_urls[i];
        } finally {
            runlock();
        }
    }

    /**
     * @param i indext of the "to" url in the array
     * @return the to_urls
     */
    private SrmUrl getTo_url(int i) {
        rlock();
        try {
            return to_urls[i];
        } finally {
            runlock();
        }
    }

    /**
     * @return the number_of_file_reqs
     */
    private int getNumber_of_file_reqs() {
        rlock();
        try {
            return number_of_file_reqs;
        } finally {
            runlock();
        }
    }

    /**
     * @param number_of_file_reqs the number_of_file_reqs to set
     */
    private void setNumber_of_file_reqs(int number_of_file_reqs) {
        wlock();
        try {
            this.number_of_file_reqs = number_of_file_reqs;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the protocols
     */
    private String[] getProtocols() {
        rlock();
        try {
            return protocols;
        } finally {
            runlock();
        }
    }

    /**
     * @return the callerSrmProtocol
     */
    private SRMProtocol getCallerSrmProtocol() {
        rlock();
        try {
            return callerSrmProtocol;
        } finally {
            runlock();
        }
    }


    /**
     * @return the remoteSrmProtocol
     */
    private SRMProtocol getRemoteSrmProtocol() {
        rlock();
        try {
            return remoteSrmProtocol;
        } finally {
            runlock();
        }
    }

    /**
     * @param remoteSrmProtocol the remoteSrmProtocol to set
     */
    private void setRemoteSrmProtocol(SRMProtocol remoteSrmProtocol) {
        wlock();
        try {
            this.remoteSrmProtocol = remoteSrmProtocol;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the remoteSrmGet
     */
    private boolean isRemoteSrmGet() {
        rlock();
        try {
            return remoteSrmGet;
        } finally {
            runlock();
        }
    }

    /**
     * @param remoteSrmGet the remoteSrmGet to set
     */
    private void setRemoteSrmGet(boolean remoteSrmGet) {
        wlock();
        try {
            this.remoteSrmGet = remoteSrmGet;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the targetSpaceToken
     */
    private String getTargetSpaceToken() {
        rlock();
        try {
            return targetSpaceToken;
        } finally {
            runlock();
        }
    }

    /**
     * @return the getter_putter
     */
    private TurlGetterPutter getGetter_putter() {
        rlock();
        try {
            return getter_putter;
        } finally {
            runlock();
        }
    }

    /**
     * @param getter_putter the getter_putter to set
     */
    private void setGetter_putter(TurlGetterPutter getter_putter) {
        wlock();
        try {
            this.getter_putter = getter_putter;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the qosPlugin
     */
    private QOSPlugin getQosPlugin() {
        rlock();
        try {
            return qosPlugin;
        } finally {
            runlock();
        }
    }

    /**
     * @return the processingDone
     */
    private boolean isProcessingDone() {
        rlock();
        try {
            return processingDone;
        } finally {
            runlock();
        }
    }

    /**
     * @param processingDone the processingDone to set
     */
    private void setProcessingDone(boolean processingDone) {
        wlock();
        try {
            this.processingDone = processingDone;
        } finally {
            wunlock();
        }
    }
}

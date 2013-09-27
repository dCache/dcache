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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import javax.annotation.Nonnull;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMProtocol;
import org.dcache.srm.SRMReleasedException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.client.RemoteTurlGetterV1;
import org.dcache.srm.client.RemoteTurlGetterV2;
import org.dcache.srm.client.RemoteTurlPutterV1;
import org.dcache.srm.client.RemoteTurlPutterV2;
import org.dcache.srm.client.RequestFailedEvent;
import org.dcache.srm.client.TURLsArrivedEvent;
import org.dcache.srm.client.TURLsGetFailedEvent;
import org.dcache.srm.client.Transport;
import org.dcache.srm.client.TurlGetterPutter;
import org.dcache.srm.client.TurlGetterPutterV1;
import org.dcache.srm.qos.QOSPlugin;
import org.dcache.srm.qos.QOSPluginFactory;
import org.dcache.srm.qos.QOSTicket;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.SrmUrl;
import org.dcache.srm.util.Tools;
import org.dcache.srm.v2_2.ArrayOfTCopyRequestFileStatus;
import org.dcache.srm.v2_2.SrmCopyResponse;
import org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TCopyRequestFileStatus;
import org.dcache.srm.v2_2.TFileStorageType;
import org.dcache.srm.v2_2.TOverwriteMode;
import org.dcache.srm.v2_2.TRequestType;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TSURLReturnStatus;


/**
 *
 * @author  timur
 */
public final class CopyRequest extends ContainerRequest<CopyFileRequest> implements PropertyChangeListener {
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

    private final Transport clientTransport;

    private transient final Multimap<String,Long> remoteSurlToFileReqIds =
            HashMultimap.create();
    private transient TurlGetterPutter getter_putter;
    private transient QOSPlugin qosPlugin;

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
        ArrayList<String> prot_list = new ArrayList<>(4);

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

        clientTransport = getConfiguration().getClientTransport();

        protocols = prot_list.toArray(new String[prot_list.size()]);
        int reqs_num = from_urls.length;
        if(reqs_num != to_urls.length) {
            logger.error("Request createCopyRequest : "+
            "unequal number of elements in url arrays");
            throw new IllegalArgumentException(
            "unequal number of elements in url arrays");
        }
        List<CopyFileRequest> requests = Lists.newArrayListWithCapacity(reqs_num);
        for(int i = 0; i < reqs_num; ++i) {
            CopyFileRequest request = new CopyFileRequest(getId(),
                    requestCredentialId,from_urls[i],to_urls[i], spaceToken,
                    lifetime, max_number_of_retries);
            requests.add(request);
        }
        setFileRequests(requests);
        this.callerSrmProtocol = callerSrmProtocol;
        if (getConfiguration().getQosPluginClass()!=null) {
            this.qosPlugin = QOSPluginFactory.createInstance(getConfiguration());
        }
        this.storageType = storageType;
        this.targetAccessLatency = targetAccessLatency;
        this.targetRetentionPolicy = targetRetentionPolicy;
        this.overwriteMode = overwriteMode;
        this.targetSpaceToken = spaceToken;
        logger.debug("Request.createCopyRequest : created new request succesfully");
    }

    /**
     * restore constructor
     */
    public  CopyRequest(
    long id,
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
    CopyFileRequest[] fileRequest,
    int retryDeltaTime,
    boolean should_updateretryDeltaTime,
    String description,
    String client_host,
    String statusCodeString,
    TFileStorageType storageType,
    TRetentionPolicy targetRetentionPolicy,
    TAccessLatency targetAccessLatency) {
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

        ArrayList<String> prot_list = new ArrayList<>(4);

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

        clientTransport = getConfiguration().getClientTransport();

        protocols = prot_list.toArray(new String[prot_list.size()]);
        if (getConfiguration().getQosPluginClass()!=null) {
            this.qosPlugin = QOSPluginFactory.createInstance(getConfiguration());
        }
        this.storageType = storageType;
        this.targetAccessLatency = targetAccessLatency;
        this.targetRetentionPolicy = targetRetentionPolicy;
        this.overwriteMode = null;
     }


    public void proccessRequest() throws DataAccessException, IOException,
            SRMException, InterruptedException, IllegalStateTransition,
            FatalJobFailure
    {
        logger.debug("Proccessing request");
        if( getNumOfFileRequest() == 0) {
            try {
                setState(State.FAILED,"Request contains zero file requests");
                return;
            }
            catch(IllegalStateTransition ist)
            {
                logger.error("Illegal State Transition : " +ist.getMessage());
            }

        }
        setNumber_of_file_reqs(getNumOfFileRequest());
        logger.debug("number_of_file_reqs = "+getNumber_of_file_reqs());
        wlock();
        try {
            List<CopyFileRequest> requests = getFileRequests();
            from_urls = new SrmUrl[getNumber_of_file_reqs()];
            to_urls = new SrmUrl[getNumber_of_file_reqs()];
            for(int i = 0 ; i<getNumber_of_file_reqs();++i) {
                CopyFileRequest cfr = requests.get(i);

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
                    logger.error(err);
                    throw new IOException(err);
                }

                if(
                ! to_urls[i].getProtocol().equals(to_urls[0].getProtocol()) ||
                ! to_urls[i].getHost().equals(to_urls[0].getHost()) ||
                ! (to_urls[i].getPort() == to_urls[0].getPort()) ) {
                    String err = "dest url #"+i+" "+to_urls[i].getURL()+" and "+
                    "dest url #0"+to_urls[0].getURL()+" are not compartible";
                    logger.error(err);
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
                            URI.create(from_urls[0].getURL()));
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
                            URI.create(to_urls[0].getURL()));
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
            CopyFileRequest cfr = getFileRequests().get(i);
            RequestCredential credential = getCredential();
            QOSTicket qosTicket = getQosPlugin().createTicket(
                    credential.getCredentialName(),
                    getStorage().getFileMetaData(getUser(), cfr.getFrom_surl(), false).size,
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
    IOException,InterruptedException,IllegalStateTransition, DataAccessException,
    FatalJobFailure {
        logger.debug("getTURLS()");
        if(isFrom_url_is_srm() && ! isFrom_url_is_local()) {
            // this means that the from url is remote srm url
            // and a "to" url is a local srm url
            if(getStorageType() != null && !storageType.equals(TFileStorageType.PERMANENT)) {
                  throw new
                      FatalJobFailure(
                      "TargetFileStorageType "+getStorageType()+" is not supported");
            }
            RequestCredential credential = getCredential();
            logger.debug("obtained credential="+credential+" id="+credential.getId());
            //String ls_client = "SRM"; // make it not hard coded

            for(int i = 0 ; i<getNumber_of_file_reqs();++i) {
                CopyFileRequest cfr = getFileRequests().get(i);
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }
                if(cfr.getFrom_turl() != null ) {
                    Scheduler.getScheduler(schedulerId).schedule(cfr);
                    continue;
                }

                //Since "to" url has to be local srm, we can just set the local to path
                remoteSurlToFileReqIds.put(getFrom_url(i).getURL(),cfr.getId());
                logger.debug("getTurlsArrived, setting local \"to\" path to "+
                cfr.getToPath());
                cfr.setLocal_to_path(
                cfr.getToPath());
                cfr.saveJob();
            }
            String[] remoteSurlsUniqueArray =
                    remoteSurlToFileReqIds.keySet()
                            .toArray(new String[remoteSurlToFileReqIds.size()]);
            for(int i=0;i<remoteSurlsUniqueArray.length;++i) {
                logger.debug("remoteSurlsUniqueArray["+i+"]="+remoteSurlsUniqueArray[i]);
            }
            //need to get from remote srm system
            setRemoteSrmGet(true);
            if(getCallerSrmProtocol() == null || getCallerSrmProtocol() == SRMProtocol.V1_1) {
                 try {
                    setGetter_putter(new RemoteTurlGetterV1(getStorage(),
                            credential, remoteSurlsUniqueArray, getProtocols(),
                            this, getConfiguration().getCopyRetryTimeout(), 2,
                            clientTransport));
                    getGetter_putter().getInitialRequest();
                    setRemoteSrmProtocol(SRMProtocol.V1_1);
                 }
                 catch(SRMException srme) {
                     logger.error("connecting to server using version 1.1 protocol failed, trying version 2.1.1");
                    setGetter_putter(new RemoteTurlGetterV2(getStorage(),
                            credential, remoteSurlsUniqueArray, getProtocols(),
                            this, getConfiguration().getCopyRetryTimeout(), 2,
                            this.getRemainingLifetime(), clientTransport));
                    getGetter_putter().getInitialRequest();
                    setRemoteSrmProtocol(SRMProtocol.V2_1);
                 }
            } else if ( getCallerSrmProtocol() == SRMProtocol.V2_1) {
                 try{
                    setGetter_putter(new RemoteTurlGetterV2(getStorage(),
                            credential, remoteSurlsUniqueArray, getProtocols(),
                            this, getConfiguration().getCopyRetryTimeout(), 2,
                            this.getRemainingLifetime(), clientTransport));
                    getGetter_putter().getInitialRequest();
                    setRemoteSrmProtocol(SRMProtocol.V2_1);
                 }
                 catch(SRMException srme) {
                     logger.error("connecting to server using version 2.1.1 protocol failed, trying version 1.1");
                    setGetter_putter(new RemoteTurlGetterV1(getStorage(),
                            credential, remoteSurlsUniqueArray, getProtocols(),
                            this, getConfiguration().getCopyRetryTimeout(), 2,
                            clientTransport));
                    getGetter_putter().getInitialRequest();
                    setRemoteSrmProtocol(SRMProtocol.V1_1);
                 }

             } else {
                 throw new FatalJobFailure("usupported srm protocol");
             }

            getGetter_putter().run();

             return;

         }

        if(isFrom_url_is_srm()) // from_url_is_local is true (nonlocal case handled above)
         {
             // this means that the from url is loacal srm url.
            for(int i = 0;i<this.getNumber_of_file_reqs();++i) {
                CopyFileRequest cfr = getFileRequests().get(i);
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
                CopyFileRequest cfr = getFileRequests().get(i);
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }

                if(cfr.getFrom_turl() != null ) {
                    continue;
                }
                logger.debug("getTurlsArrived, setting \"from\" turl to "+
                getFrom_url(i).getURL());
                cfr.setFrom_turl(URI.create(getFrom_url(i).getURL()));
                cfr.saveJob();

            }

        }

        // now "from" turl or local path is known, need to handle the "to" part

        if(isTo_url_is_srm() &&  isTo_url_is_local()) {
            //this means that we either local "from" srm url or
            // non local "from" turl, and we have local to srm
            // we have all info needed to proccede
            for(int i = 0;i<this.getNumber_of_file_reqs();++i) {
                CopyFileRequest cfr = getFileRequests().get(i);
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
                CopyFileRequest cfr = getFileRequests().get(i);
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }
                logger.debug("getTurlsArrived, setting remote \"to\" TURL to "+
                getTo_url(i).getURL());
                cfr.setTo_turl(URI.create(getTo_url(i).getURL()));
                cfr.saveJob();
                // everything is known, can start transfers
                Scheduler.getScheduler(schedulerId).schedule(cfr);
            }
            return;
        }
        // the only case remaining is the local "from" srm url
        //the to url is a remote srm url -> need to discover "to" TURL

        for(int i = 0;i<this.getNumber_of_file_reqs();++i) {
            CopyFileRequest cfr = getFileRequests().get(i);
            if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                // copy file request has being canceled,failed or scheduled before
                continue;
            }

            if(cfr.getTo_turl() != null ) {
                //to turl has arrived , but request has not been scheduled
                Scheduler.getScheduler(schedulerId).schedule(cfr);
                continue;
            }

            remoteSurlToFileReqIds.put(getTo_url(i).getURL(),cfr.getId());
        }

        String[] remoteSurlsUniqueArray =
                remoteSurlToFileReqIds.keySet()
                        .toArray(new String[remoteSurlToFileReqIds.size()]);
        int length = remoteSurlsUniqueArray.length;
        String[] dests = new String[length];
        long[] sizes = new long[length];
        for(int i =0 ; i<length;++i) {
            long fileRequestId = Iterables.get(remoteSurlToFileReqIds.get(remoteSurlsUniqueArray[i]), 0);
            CopyFileRequest cfr = getFileRequest(fileRequestId);
            sizes[i] = getStorage().getFileMetaData(getUser(), cfr.getFrom_surl(), false).size;
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
                setGetter_putter(new RemoteTurlPutterV1(getStorage(),
                        credential, dests, sizes, getProtocols(), this,
                        getConfiguration().getCopyRetryTimeout(), 2,
                        clientTransport));
                getGetter_putter().getInitialRequest();
                setRemoteSrmProtocol(SRMProtocol.V1_1);
             }
             catch(SRMException srme) {
                 logger.error("connecting to server using version 1.1 protocol failed, trying version 2.1.1");
                 setGetter_putter(new RemoteTurlPutterV2(getStorage(),
                         credential, dests, sizes, getProtocols(), this,
                         getConfiguration().getCopyRetryTimeout(), 2,
                         this.getRemainingLifetime(), getStorageType(),
                         getTargetRetentionPolicy(), getTargetAccessLatency(),
                         getOverwriteMode(), getTargetSpaceToken(),
                         clientTransport));
                 getGetter_putter().getInitialRequest();
                 setRemoteSrmProtocol(SRMProtocol.V2_1);
             }
        } else if ( getCallerSrmProtocol() == SRMProtocol.V2_1) {
            try {
                setGetter_putter(new RemoteTurlPutterV2(getStorage(),
                        credential, dests, sizes, getProtocols(), this,
                        getConfiguration().getCopyRetryTimeout(), 2,
                        this.getRemainingLifetime(), getStorageType(),
                        getTargetRetentionPolicy(), getTargetAccessLatency(),
                        getOverwriteMode(), getTargetSpaceToken(),
                        clientTransport));
                getGetter_putter().getInitialRequest();
                setRemoteSrmProtocol(SRMProtocol.V2_1);
             }
             catch(SRMException srme) {
                 logger.error("connecting to server using version 2.1.1 protocol failed, trying version 1.1");
                setGetter_putter(new RemoteTurlPutterV1(getStorage(),
                        credential, dests, sizes, getProtocols(), this,
                        getConfiguration().getCopyRetryTimeout(), 2,
                        clientTransport));
                getGetter_putter().getInitialRequest();
                setRemoteSrmProtocol(SRMProtocol.V1_1);
             }
         } else {
             throw new FatalJobFailure("usupported srm protocol");
         }

         getGetter_putter().run();
    }

    public void turlArrived(String SURL, String TURL,String remoteRequestId,String remoteFileId,Long size) {

        synchronized(remoteSurlToFileReqIds) {
            Collection<Long> fileRequestSet = remoteSurlToFileReqIds.get(SURL);
            if(fileRequestSet == null || fileRequestSet.isEmpty()) {
                logger.error("turlArrived for unknown SURL = "+SURL+" !!!!!!!");
                return;
            }
            Long[] cfr_ids = fileRequestSet
                    .toArray(new Long[fileRequestSet.size()]);
            for (long cfr_id : cfr_ids) {

                CopyFileRequest cfr = getFileRequest(cfr_id);
                if (getQosPlugin() != null && cfr.getQOSTicket() != null) {
                    getQosPlugin().sayStatus(cfr.getQOSTicket());
                }

                if (isFrom_url_is_srm() && !isFrom_url_is_local()) {
                    cfr.setFrom_turl(URI.create(TURL));
                } else {
                    cfr.setTo_turl(URI.create(TURL));
                }
                if (size != null) {
                    cfr.setSize(size);
                }
                cfr.setRemoteRequestId(remoteRequestId);
                cfr.setRemoteFileId(remoteFileId);
                cfr.saveJob();


                try {
                    String theShedulerId = schedulerId;
                    State file_state = cfr.getState();
                    if (theShedulerId != null &&
                            !(file_state == State.CANCELED || file_state == State.FAILED || file_state == State.DONE)) {
                        Scheduler.getScheduler(theShedulerId).schedule(cfr);
                    }
                } catch (Exception e) {
                    logger.error(e.toString());
                    logger.error("failed to schedule CopyFileRequest " + cfr);
                    try {
                        cfr.setState(State.FAILED, "failed to schedule CopyFileRequest " + cfr + " rasaon: " + e);
                    } catch (IllegalStateTransition ist) {
                        logger.error("Illegal State Transition : " + ist
                                .getMessage());
                    }
                }
                remoteSurlToFileReqIds.remove(SURL, cfr_id);
            }
        }
    }

    public void turlRetrievalFailed(String SURL, String reason,String remoteRequestId,String remoteFileId) {

        synchronized(remoteSurlToFileReqIds) {
            Collection<Long> fileRequestSet = remoteSurlToFileReqIds.get(SURL);
            if(fileRequestSet == null || fileRequestSet.isEmpty()) {
                logger.error("turlArrived for unknown SURL = "+SURL);
                return;
            }
            Long[] cfr_ids = fileRequestSet
                    .toArray(new Long[fileRequestSet.size()]);
            for (long cfr_id : cfr_ids) {

                CopyFileRequest cfr = getFileRequest(cfr_id);

                try {
                    String error;
                    if (isFrom_url_is_srm() && !isFrom_url_is_local()) {
                        error = "retrieval of \"from\" TURL failed with error " + reason;
                    } else {
                        error = "retrieval of \"to\" TURL failed with error " + reason;
                    }
                    logger.error(error);
                    cfr.setState(State.FAILED, error);
                } catch (IllegalStateTransition ist) {
                    logger.error("Illegal State Transition : " + ist
                            .getMessage());
                }
                cfr.saveJob();

                remoteSurlToFileReqIds.remove(SURL, cfr_id);
            }
        }
        remoteFileRequestDone(SURL,remoteRequestId,remoteFileId);

    }

    public void turlsRetrievalFailed(Object reason) {
        synchronized(remoteSurlToFileReqIds) {
            String SURLs[] = remoteSurlToFileReqIds.keySet()
                    .toArray(new String[remoteSurlToFileReqIds.size()]);
            for (String surl : SURLs) {
                Collection<Long> fileRequestSet = remoteSurlToFileReqIds.get(surl);
                Long[] cfr_ids = fileRequestSet
                        .toArray(new Long[fileRequestSet.size()]);
                for (long cfr_id : cfr_ids) {
                    CopyFileRequest cfr = getFileRequest(cfr_id);
                    try {
                        String error;
                        if (isFrom_url_is_srm() && !isFrom_url_is_local()) {

                            error = "retrieval of \"from\" TURL failed with error " + reason;
                        } else {
                            error = "retrieval of \"to\" TURL failed with error " + reason;
                        }
                        logger.error(error);
                        cfr.setState(State.FAILED, error);
                    } catch (IllegalStateTransition ist) {
                        logger.error("Illegal State Transition : " + ist
                                .getMessage());
                    }
                    cfr.saveJob();
                    remoteSurlToFileReqIds.remove(surl, cfr_id);
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
                            getConfiguration().getCopyMaxNumOfRetries(),
                            clientTransport);
                } else if( getRemoteSrmProtocol() == SRMProtocol.V2_1) {
                    if(isRemoteSrmGet())
                    {
                       RemoteTurlGetterV2.staticReleaseFile(getCredential(),
                               SURL, remoteRequestIdString,
                               getConfiguration().getCopyRetryTimeout(),
                               getConfiguration().getCopyMaxNumOfRetries(),
                               clientTransport);
                    } else {
                        RemoteTurlPutterV2.staticPutDone(getCredential(),
                               SURL, remoteRequestIdString,
                               getConfiguration().getCopyRetryTimeout(),
                               getConfiguration().getCopyMaxNumOfRetries(),
                               clientTransport);
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
    throws SRMException{
        if(fromurl == null || tourl == null) {
           throw new SRMException("surl is null");
        }
        for (CopyFileRequest request : getFileRequests()) {
            if(request.getFromURL().equals(fromurl) &&
               request.getToURL().equals(tourl)) {
                return request;
            }
        }
        throw new SRMException("file request for from url ="+fromurl+
        " and to url="+tourl +" is not found");
    }


    @Override
    public String getMethod() {
        return "Copy";
    }

    //we want to stop handler if the
    //the request is ready (all file reqs are ready), since all copy transfers are
    // competed by now
    public boolean shouldStopHandlerIfReady() {
        return true;
    }

   private volatile boolean processingDone;

    @Override
    public void run() throws NonFatalJobFailure, FatalJobFailure {
        if(isProcessingDone())
        {
            return;
        }
        try
        {
            proccessRequest();
            boolean done = true;
            for (CopyFileRequest request : getFileRequests()) {
                State state = request.getState();
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
        } catch(SRMException | IllegalStateTransition | DataAccessException e) {
            // FIXME some SRMException failures are temporary and others are
            // permanent.  Code currently doesn't distinguish between them and
            // always retries, even if problem isn't transitory.
            throw new NonFatalJobFailure(e.toString());
        } catch(IOException | InterruptedException e) {
            throw new FatalJobFailure(e.toString());
        }
    }

    @Override
    protected void stateChanged(State oldState) {
        State state = getState();
        if(State.isFinalState(state)) {

            TurlGetterPutter a_getter_putter = getGetter_putter();
            if(a_getter_putter != null) {
                logger.debug("copyRequest getter_putter is non null, stopping");
                a_getter_putter.stop();
            }
            logger.debug("copy request state changed to "+state);
            for (CopyFileRequest request : getFileRequests()) {
                try {
                    State fr_state = request.getState();
                    if(!(State.isFinalState(fr_state)))
                    {

                        logger.debug("changing fr#"+request.getId()+" to "+state);
                            request.setState(state,"Request state changed, changing file state");
                    }
                }
                catch(IllegalStateTransition ist) {
                    logger.error("Illegal State Transition : " +ist.getMessage());
                }
            }

        }

    }

    @Override
    public void propertyChange(PropertyChangeEvent evt) {
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
                for (CopyFileRequest request : getFileRequests()) {
                    State state = request.getState();
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
        throws SRMException {
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
        return String.valueOf(getId());
    }

    public final TCopyRequestFileStatus[]  getArrayOfTCopyRequestFileStatuses(
        String[] fromurls,String[] tourls)
        throws SRMException {
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
                    CopyFileRequest fr = getFileRequests().get(i);
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
        throws SRMException
    {
        return getSrmStatusOfCopyRequest(null,null);
    }

    public final SrmStatusOfCopyRequestResponse getSrmStatusOfCopyRequest(String[] fromurls,String[] tourls)
        throws SRMException {
        SrmStatusOfCopyRequestResponse response = new SrmStatusOfCopyRequestResponse();
        response.setReturnStatus(getTReturnStatus());
        ArrayOfTCopyRequestFileStatus arrayOfTCopyRequestFileStatus =
            new ArrayOfTCopyRequestFileStatus();
        arrayOfTCopyRequestFileStatus.setStatusArray(
            getArrayOfTCopyRequestFileStatuses(fromurls,tourls));
        response.setArrayOfFileStatuses(arrayOfTCopyRequestFileStatus);
        return response;
    }

    @Nonnull
    @Override
    public final CopyFileRequest getFileRequestBySurl(URI surl) throws SRMFileRequestNotFoundException
    {
        for (CopyFileRequest request : getFileRequests()) {
            if(request.getFrom_surl().equals(surl) ||
               request.getTo_surl().equals(surl) ) {
                return request;
            }
        }
        throw new SRMFileRequestNotFoundException("file request for url =" + surl + " is not found");
    }

    @Override
    public final TSURLReturnStatus[] getArrayOfTSURLReturnStatus(URI[] surls) throws SRMException {
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
                CopyFileRequest fr = getFileRequests().get(i);
                surlLReturnStatuses[i] = fr.getTSURLReturnStatus( null);
            }
        } else {
            for(int i = 0; i< len; ++i) {
                CopyFileRequest fr = getFileRequestBySurl(surls[i]);
                surlLReturnStatuses[i] = fr.getTSURLReturnStatus(surls[i]);
            }

        }
        return surlLReturnStatuses;
    }

    @Override
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
        /* [ SRM 2.2, 5.16.2 ]
         *
         * h) Lifetime cannot be extended on the released files, aborted files, expired
         *    files, and suspended files. For example, pin lifetime cannot be extended
         *    after srmPutDone is requested on SURLs for srmPrepareToPut request. In
         *    such case, SRM_INVALID_REQUEST at the file level must be returned, and
         *    SRM_PARTIAL_SUCCESS or SRM_FAILURE must be returned at the request level.
         */
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
